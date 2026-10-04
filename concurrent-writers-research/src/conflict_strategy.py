"""
conflict_strategy.py
====================
Task 3: Cài đặt Conflict Resolution Strategies

Strategies:
  Baseline         — Delta Lake OCC mặc định, không can thiệp
  Strategy 1       — Exponential Backoff + Jitter (chống thundering herd)
  Strategy 2       — Selective Serialization (chỉ serialize writer có overlap key)

Mỗi strategy được wrap vào 1 lớp riêng kế thừa từ ConflictStrategy (ABC).
Metrics được log ra file: results/metrics_<strategy>_<pattern>_<timestamp>.json

Chạy benchmark:
  python src/conflict_strategy.py
"""

import os
import uuid
import time
import json
import math
import random
import logging
import threading
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from typing import List, Dict, Any, Optional, Callable, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed, Future
from queue import Queue

import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from deltalake.exceptions import CommitFailedError

# ─── LOGGING ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(threadName)s] %(levelname)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# ─── PATHS ─────────────────────────────────────────────────────────────────────
BASE_DIR    = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TABLE_PATH  = os.path.join(BASE_DIR, "data", "transactions")
RESULTS_DIR = os.path.join(BASE_DIR, "results")
ALL_PARTITIONS = [
    f"2024-01-0{i}" if i < 10 else f"2024-01-{i}"
    for i in range(1, 9)
]
STATUSES = ["pending", "completed", "failed"]


# ─── METRICS ───────────────────────────────────────────────────────────────────

@dataclass
class TransactionMetric:
    """Metric cho 1 transaction attempt."""
    transaction_id:  str
    writer_id:       str
    strategy:        str
    pattern:         str
    attempt_number:  int    # 1-indexed, >1 nếu retry
    committed:       bool
    latency_ms:      float
    commit_version:  Optional[int]
    target_partition: str
    timestamp:       float


@dataclass
class RunSummary:
    """Tóm tắt 1 lần chạy experiment."""
    strategy:             str
    pattern:              str
    num_writers:          int
    num_rows_per_writer:  int
    conflict_ratio:       float
    elapsed_seconds:      float
    total_rows_committed: int
    throughput_rows_per_sec: float
    conflict_rate:        float
    retry_count_total:    int
    abort_count:          int
    p50_latency_ms:       float
    p95_latency_ms:       float
    stddev_latency_ms:    float


# ─── HELPER FUNCTIONS ──────────────────────────────────────────────────────────

def make_payload(partition_date: str, num_rows: int) -> pd.DataFrame:
    rows = []
    start_id = random.randint(1_000_000, 9_000_000)
    for i in range(num_rows):
        rows.append({
            "id":             start_id + i,
            "user_id":        random.randint(1, 200),
            "amount":         round(random.uniform(10.0, 1000.0), 2),
            "status":         random.choice(STATUSES),
            "partition_date": partition_date,
            "created_at":     time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        })
    return pd.DataFrame(rows)


def percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(len(sorted_data) * p / 100)
    idx = min(idx, len(sorted_data) - 1)
    return sorted_data[idx]


def stddev(data: List[float]) -> float:
    if len(data) < 2:
        return 0.0
    mean = sum(data) / len(data)
    variance = sum((x - mean) ** 2 for x in data) / len(data)
    return math.sqrt(variance)


# ─── ABC BASE STRATEGY ─────────────────────────────────────────────────────────

class ConflictStrategy(ABC):
    """
    Abstract base class cho tất cả conflict resolution strategies.
    Mỗi subclass implement `execute_write()`.
    """

    def __init__(self, strategy_name: str, table_path: str = TABLE_PATH):
        self.strategy_name = strategy_name
        self.table_path    = table_path
        self.metrics: List[TransactionMetric] = []
        self._lock = threading.Lock()

    @abstractmethod
    def execute_write(
        self,
        writer_id: str,
        df: pd.DataFrame,
        partition: str,
        pattern: str,
    ) -> TransactionMetric:
        """
        Thực hiện 1 write operation với conflict resolution cụ thể.
        Trả về TransactionMetric.
        """
        ...

    def _raw_commit(self, df: pd.DataFrame) -> Tuple[bool, Optional[int], float]:
        """Thực hiện commit thực sự vào Delta Lake. Trả về (success, version, latency_ms)."""
        t0 = time.time()
        try:
            write_deltalake(self.table_path, df, mode="append", schema_mode="merge")
            dt = DeltaTable(self.table_path)
            return True, dt.version(), (time.time() - t0) * 1000
        except (CommitFailedError, Exception) as e:
            log.debug(f"  Raw commit failed: {type(e).__name__}: {e}")
            return False, None, (time.time() - t0) * 1000

    def collect_metric(self, m: TransactionMetric) -> None:
        with self._lock:
            self.metrics.append(m)

    def compute_summary(
        self,
        pattern: str,
        num_writers: int,
        num_rows_per_writer: int,
        conflict_ratio: float,
        elapsed: float,
    ) -> RunSummary:
        latencies     = [m.latency_ms for m in self.metrics]
        committed     = [m for m in self.metrics if m.committed]
        total_rows    = len(committed) * num_rows_per_writer
        retries_total = sum(m.attempt_number - 1 for m in self.metrics)
        aborts        = sum(1 for m in self.metrics if not m.committed)
        n_attempts    = len(self.metrics)
        conflict_rate = aborts / max(n_attempts, 1)

        return RunSummary(
            strategy             = self.strategy_name,
            pattern              = pattern,
            num_writers          = num_writers,
            num_rows_per_writer  = num_rows_per_writer,
            conflict_ratio       = conflict_ratio,
            elapsed_seconds      = round(elapsed, 3),
            total_rows_committed = total_rows,
            throughput_rows_per_sec = round(total_rows / max(elapsed, 0.001), 2),
            conflict_rate        = round(conflict_rate, 4),
            retry_count_total    = retries_total,
            abort_count          = aborts,
            p50_latency_ms       = round(percentile(latencies, 50), 2),
            p95_latency_ms       = round(percentile(latencies, 95), 2),
            stddev_latency_ms    = round(stddev(latencies), 2),
        )


# ─── STRATEGY: BASELINE ────────────────────────────────────────────────────────

class BaselineStrategy(ConflictStrategy):
    """
    Task 3.1: Baseline — Cấu hình mặc định Delta Lake OCC.
    Không có retry, không can thiệp. Đây là "control group".
    """

    def __init__(self, table_path: str = TABLE_PATH):
        super().__init__("Baseline", table_path)

    def execute_write(self, writer_id, df, partition, pattern) -> TransactionMetric:
        txn_id = str(uuid.uuid4())
        t0     = time.time()

        success, version, latency = self._raw_commit(df)

        m = TransactionMetric(
            transaction_id   = txn_id,
            writer_id        = writer_id,
            strategy         = self.strategy_name,
            pattern          = pattern,
            attempt_number   = 1,
            committed        = success,
            latency_ms       = latency,
            commit_version   = version,
            target_partition = partition,
            timestamp        = t0,
        )
        self.collect_metric(m)
        return m


# ─── STRATEGY 1: EXPONENTIAL BACKOFF + JITTER ──────────────────────────────────

class ExponentialBackoffStrategy(ConflictStrategy):
    """
    Task 3.2: Exponential Backoff + Full Jitter.
    Công thức: wait = min(cap, base * 2^attempt) + random.uniform(0, jitter)

    Giúp tránh "thundering herd" — các writer không retry cùng lúc.
    """

    def __init__(
        self,
        table_path:  str   = TABLE_PATH,
        base:        float = 0.1,     # 100ms base wait
        cap:         float = 10.0,    # Max 10s wait
        jitter:      float = 0.5,     # Max jitter 500ms
        max_retries: int   = 5,
    ):
        super().__init__("ExponentialBackoff+Jitter", table_path)
        self.base        = base
        self.cap         = cap
        self.jitter      = jitter
        self.max_retries = max_retries

    def _compute_wait(self, attempt: int) -> float:
        """Task 3.2: wait = min(cap, base * 2^attempt) + random.uniform(0, jitter)"""
        exponential_wait = min(self.cap, self.base * (2 ** attempt))
        jitter_component = random.uniform(0, self.jitter)
        return exponential_wait + jitter_component

    def execute_write(self, writer_id, df, partition, pattern) -> TransactionMetric:
        txn_id  = str(uuid.uuid4())
        t0      = time.time()
        attempt = 0
        success = False
        version = None

        while attempt <= self.max_retries:
            ok, ver, _ = self._raw_commit(df)
            if ok:
                success = True
                version = ver
                break

            attempt += 1
            if attempt > self.max_retries:
                log.warning(f"[{writer_id}] MaxRetries={self.max_retries} đã hết, abort.")
                break

            wait = self._compute_wait(attempt)
            log.debug(f"[{writer_id}] Retry #{attempt}, chờ {wait:.3f}s...")
            time.sleep(wait)

        latency = (time.time() - t0) * 1000
        m = TransactionMetric(
            transaction_id   = txn_id,
            writer_id        = writer_id,
            strategy         = self.strategy_name,
            pattern          = pattern,
            attempt_number   = attempt + 1,
            committed        = success,
            latency_ms       = latency,
            commit_version   = version,
            target_partition = partition,
            timestamp        = t0,
        )
        self.collect_metric(m)
        return m


# ─── STRATEGY 2: SELECTIVE SERIALIZATION ───────────────────────────────────────

class KeyConflictCoordinator:
    """
    Task 3.3: Bộ điều phối kiểm tra overlap key.
    - Nếu 2 writer cùng ghi vào 1 partition → queue (serialized)
    - Nếu không overlap → thả song song
    """

    def __init__(self):
        self._active_partitions: Dict[str, threading.Lock] = {}
        self._global_lock = threading.Lock()

    def acquire(self, partition: str) -> threading.Lock:
        """Lấy lock cho partition cụ thể. Tạo mới nếu chưa có."""
        with self._global_lock:
            if partition not in self._active_partitions:
                self._active_partitions[partition] = threading.Lock()
            return self._active_partitions[partition]

    def has_conflict(self, partition: str) -> bool:
        """Kiểm tra xem partition có đang bị lock không."""
        with self._global_lock:
            lock = self._active_partitions.get(partition)
            if lock is None:
                return False
            return lock.locked()


class SelectiveSerializationStrategy(ConflictStrategy):
    """
    Task 3.3: Selective Serialization.
    Coordinator kiểm tra key overlap:
      - Overlap → serialize (chờ lock)
      - No overlap → parallel (không bị block)

    Hiệu quả nhất khi conflict_ratio thấp.
    """

    def __init__(self, table_path: str = TABLE_PATH):
        super().__init__("SelectiveSerialization", table_path)
        self.coordinator = KeyConflictCoordinator()

    def execute_write(self, writer_id, df, partition, pattern) -> TransactionMetric:
        txn_id = str(uuid.uuid4())
        t0     = time.time()

        # Kiểm tra overlap
        had_conflict = self.coordinator.has_conflict(partition)
        partition_lock = self.coordinator.acquire(partition)

        with partition_lock:   # Chỉ 1 writer được ghi vào partition tại 1 thời điểm
            if had_conflict:
                log.debug(f"[{writer_id}] ⏳ Queue vì partition {partition} bị lock")
            success, version, _ = self._raw_commit(df)

        latency = (time.time() - t0) * 1000
        m = TransactionMetric(
            transaction_id   = txn_id,
            writer_id        = writer_id,
            strategy         = self.strategy_name,
            pattern          = pattern,
            attempt_number   = 1,   # Selective serialization không retry, chờ thay thế
            committed        = success,
            latency_ms       = latency,
            commit_version   = version,
            target_partition = partition,
            timestamp        = t0,
        )
        self.collect_metric(m)
        return m


# ─── IDEMPOTENT COMMIT ID ──────────────────────────────────────────────────────

class IdempotentWriter:
    """
    Task 3.4: Gắn Commit ID (UUID) vào mỗi transaction.
    Tránh duplicate khi retry sau timeout.

    Trong môi trường thực tế, UUID được lưu trong Delta Log user metadata.
    """

    def __init__(self):
        self._committed_ids: set = set()
        self._lock = threading.Lock()

    def is_duplicate(self, txn_id: str) -> bool:
        with self._lock:
            return txn_id in self._committed_ids

    def mark_committed(self, txn_id: str) -> None:
        with self._lock:
            self._committed_ids.add(txn_id)

    def safe_commit(
        self,
        txn_id: str,
        table_path: str,
        df: pd.DataFrame,
    ) -> Tuple[bool, Optional[int]]:
        """Commit idempotent: nếu txn_id đã committed, bỏ qua."""
        if self.is_duplicate(txn_id):
            log.warning(f"  Idempotent: txn {txn_id[:8]}... đã committed, bỏ qua duplicate")
            return False, None

        try:
            write_deltalake(table_path, df, mode="append", schema_mode="merge")
            dt = DeltaTable(table_path)
            self.mark_committed(txn_id)
            return True, dt.version()
        except Exception as e:
            log.debug(f"  Idempotent commit failed: {e}")
            return False, None


# ─── BENCHMARK RUNNER ──────────────────────────────────────────────────────────

class BenchmarkRunner:
    """
    Task 3.5: Chạy benchmark so sánh Baseline vs Strategy 1 vs Strategy 2.
    Log đầy đủ metrics ra file JSON.
    """

    def __init__(
        self,
        table_path:          str   = TABLE_PATH,
        results_dir:         str   = RESULTS_DIR,
        num_writers:         int   = 4,
        num_rows_per_writer: int   = 30,
        conflict_ratio:      float = 1.0,
        pattern:             str   = "B",
    ):
        self.table_path          = table_path
        self.results_dir         = results_dir
        self.num_writers         = num_writers
        self.num_rows_per_writer = num_rows_per_writer
        self.conflict_ratio      = conflict_ratio
        self.pattern             = pattern
        os.makedirs(results_dir, exist_ok=True)

    def _get_partition(self, writer_idx: int) -> str:
        """Chọn partition dựa trên pattern & conflict_ratio."""
        if self.pattern == "A":
            return ALL_PARTITIONS[writer_idx % len(ALL_PARTITIONS)]
        else:
            if random.random() < self.conflict_ratio:
                return ALL_PARTITIONS[0]   # Shared partition
            return random.choice(ALL_PARTITIONS[1:])

    def _run_strategy(self, strategy: ConflictStrategy) -> RunSummary:
        """Chạy 1 strategy với num_writers threads song song."""
        log.info(f"\n{'─'*50}")
        log.info(f"  Strategy: {strategy.strategy_name}")
        log.info(f"  Pattern: {self.pattern}, Writers: {self.num_writers}, "
                 f"conflict_ratio: {self.conflict_ratio}")
        log.info(f"{'─'*50}")

        t0 = time.time()

        def writer_task(writer_idx: int):
            partition = self._get_partition(writer_idx)
            df        = make_payload(partition, self.num_rows_per_writer)
            return strategy.execute_write(
                writer_id = f"writer_{writer_idx}",
                df        = df,
                partition = partition,
                pattern   = self.pattern,
            )

        with ThreadPoolExecutor(
            max_workers     = self.num_writers,
            thread_name_prefix = f"{strategy.strategy_name[:8]}",
        ) as exe:
            futures = [exe.submit(writer_task, i) for i in range(self.num_writers)]
            results = [f.result() for f in as_completed(futures)]

        elapsed = time.time() - t0
        summary = strategy.compute_summary(
            pattern              = self.pattern,
            num_writers          = self.num_writers,
            num_rows_per_writer  = self.num_rows_per_writer,
            conflict_ratio       = self.conflict_ratio,
            elapsed              = elapsed,
        )

        # Log ra console
        log.info(f"  ✅ {strategy.strategy_name} xong!")
        log.info(f"     Throughput:      {summary.throughput_rows_per_sec} rows/s")
        log.info(f"     Conflict rate:   {summary.conflict_rate:.1%}")
        log.info(f"     Retries total:   {summary.retry_count_total}")
        log.info(f"     Aborts:          {summary.abort_count}")
        log.info(f"     p50 latency:     {summary.p50_latency_ms}ms")
        log.info(f"     p95 latency:     {summary.p95_latency_ms}ms")

        # Task 3.5: Lưu metrics ra file
        ts = int(time.time())
        out = {
            "summary":     asdict(summary),
            "transactions": [asdict(m) for m in strategy.metrics],
        }
        fname = os.path.join(
            self.results_dir,
            f"metrics_{strategy.strategy_name.replace('+','_').replace(' ','_')}"
            f"_pattern{self.pattern}_{ts}.json"
        )
        with open(fname, "w") as f:
            json.dump(out, f, indent=2)
        log.info(f"     📄 Metrics → {fname}")

        return summary

    def run_all(self) -> List[RunSummary]:
        """So sánh tất cả strategies, trả về list RunSummary."""
        strategies = [
            BaselineStrategy(self.table_path),
            ExponentialBackoffStrategy(self.table_path),
            SelectiveSerializationStrategy(self.table_path),
        ]

        log.info(f"\n{'='*60}")
        log.info(f"🏁 BENCHMARK: Pattern={self.pattern}, Writers={self.num_writers}, "
                 f"conflict_ratio={self.conflict_ratio}")
        log.info(f"{'='*60}")

        summaries = []
        for s in strategies:
            summary = self._run_strategy(s)
            summaries.append(summary)

        # ── Bảng so sánh ──────────────────────────────────────────────────────
        log.info(f"\n{'='*60}")
        log.info(f"📊 KẾT QUẢ SO SÁNH (Pattern {self.pattern})")
        log.info(f"{'='*60}")
        header = f"{'Strategy':<30} {'Throughput':>12} {'Abort%':>8} {'p95ms':>8} {'Retries':>8}"
        log.info(header)
        log.info("─" * len(header))
        for s in summaries:
            log.info(
                f"{s.strategy:<30} "
                f"{s.throughput_rows_per_sec:>12.1f} "
                f"{s.conflict_rate:>8.1%} "
                f"{s.p95_latency_ms:>8.1f} "
                f"{s.retry_count_total:>8}"
            )

        # Lưu bảng tổng hợp
        ts = int(time.time())
        comparison_path = os.path.join(
            self.results_dir,
            f"comparison_pattern{self.pattern}_{ts}.json"
        )
        with open(comparison_path, "w") as f:
            json.dump([asdict(s) for s in summaries], f, indent=2)
        log.info(f"\n💾 Bảng so sánh → {comparison_path}")

        return summaries


# ─── MAIN ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    if not os.path.exists(os.path.join(TABLE_PATH, "_delta_log")):
        print("⚠️  Bảng chưa tồn tại. Chạy setup_environment.py trước!")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("⚔️  CONFLICT STRATEGY BENCHMARK")
    print("=" * 60)

    # Chạy benchmark với Pattern B (highest conflict scenario)
    runner = BenchmarkRunner(
        num_writers         = 4,
        num_rows_per_writer = 30,
        conflict_ratio      = 1.0,
        pattern             = "B",
    )
    runner.run_all()

    print("\n✅ Benchmark hoàn tất! Xem kết quả trong ./results/")
