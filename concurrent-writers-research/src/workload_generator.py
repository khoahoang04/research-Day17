"""
workload_generator.py
=====================
Task 2: Xây dựng Workload Generator với 3 Access Patterns bắt buộc

Patterns:
  A — Disjoint Writes:       N writers, mỗi writer ghi vào 1 partition riêng
  B — Overlapping Writes:    N writers cùng MERGE/UPDATE trên cùng partition/key
  C — Maintenance Concurrent: 1 thread OPTIMIZE + N writers Append đồng thời

Chạy demo:
  python src/workload_generator.py

Output bàn giao cho Thành viên B:
  results/commit_log_<pattern>_<timestamp>.json
    → List of {writer_id, rows_committed, timestamp, transaction_id}
"""

import os
import uuid
import time
import json
import random
import threading
import logging
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

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

# ─── PARTITIONS ────────────────────────────────────────────────────────────────
BASE_DATE       = datetime(2024, 1, 1)
ALL_PARTITIONS  = [(BASE_DATE + timedelta(days=i)).strftime("%Y-%m-%d") for i in range(8)]
STATUSES        = ["pending", "completed", "failed"]


# ─── DATA MODELS ───────────────────────────────────────────────────────────────

@dataclass
class AuditEntry:
    """Audit Log entry — bàn giao cho Thành viên B làm Oracle input."""
    transaction_id: str
    writer_id:      str
    operation:      str          # 'APPEND' | 'MERGE' | 'UPDATE' | 'OPTIMIZE'
    target_key:     str          # JSON-serialized partition or key range
    value:          Dict[str, Any]
    timestamp:      float        # Unix timestamp khi bắt đầu commit attempt
    committed:      bool
    retry_count:    int
    commit_version: Optional[int]
    latency_ms:     float


@dataclass
class WriterResult:
    """Kết quả tóm tắt sau khi 1 writer hoàn thành — bàn giao cho Thành viên B."""
    writer_id:      str
    rows_committed: int
    timestamp:      float
    transaction_ids: List[str] = field(default_factory=list)


# ─── BASE WRITER ───────────────────────────────────────────────────────────────

class Writer:
    """
    Task 2.1: Lớp Writer cơ sở.
    Nhận payload, ghi vào Delta table, trả về AuditEntry.
    """

    def __init__(self, writer_id: str, table_path: str):
        self.writer_id  = writer_id
        self.table_path = table_path

    def _make_payload(self, partition_date: str, num_rows: int,
                      start_id: int = None) -> pd.DataFrame:
        """Tạo DataFrame payload cần ghi."""
        if start_id is None:
            # ID ngẫu nhiên cao để tránh conflict PK với seed data
            start_id = random.randint(100_000, 900_000)
        rows = []
        for i in range(num_rows):
            rows.append({
                "id":             start_id + i,
                "user_id":        random.randint(1, 200),
                "amount":         round(random.uniform(10.0, 1000.0), 2),
                "status":         random.choice(STATUSES),
                "partition_date": partition_date,
                "created_at":     datetime.utcnow().isoformat(),
            })
        return pd.DataFrame(rows)

    def commit(self, df: pd.DataFrame, operation: str = "APPEND",
               target_key: str = "") -> AuditEntry:
        """
        Thực hiện commit 1 transaction.
        Trả về AuditEntry với committed=True/False.
        """
        txn_id    = str(uuid.uuid4())
        t_start   = time.time()
        committed = False
        version   = None
        retry     = 0

        try:
            write_deltalake(
                self.table_path,
                df,
                mode="append",
                schema_mode="merge",
            )
            # Lấy version mới nhất sau commit
            dt      = DeltaTable(self.table_path)
            version = dt.version()
            committed = True
            log.debug(f"[{self.writer_id}] ✅ Commit OK — v{version}, {len(df)} rows")

        except (CommitFailedError, Exception) as e:
            log.warning(f"[{self.writer_id}] ❌ Commit FAIL — {type(e).__name__}: {e}")

        latency_ms = (time.time() - t_start) * 1000

        return AuditEntry(
            transaction_id = txn_id,
            writer_id      = self.writer_id,
            operation      = operation,
            target_key     = target_key,
            value          = {
                "num_rows":    len(df),
                "sum_amount":  round(df["amount"].sum(), 2) if "amount" in df.columns else 0,
                "ids_range":   f"{df['id'].min()}–{df['id'].max()}" if "id" in df.columns else "",
            },
            timestamp      = t_start,
            committed      = committed,
            retry_count    = retry,
            commit_version = version,
            latency_ms     = latency_ms,
        )


# ─── PATTERN A — DISJOINT WRITES ───────────────────────────────────────────────

class DisjointWriter(Writer):
    """
    Task 2.2: Pattern A — Disjoint Writes.
    Mỗi writer được gán 1 partition riêng biệt → không bao giờ có conflict thực sự.
    """

    def __init__(self, writer_id: str, table_path: str, partition_date: str,
                 num_rows_per_writer: int = 50):
        super().__init__(writer_id, table_path)
        self.partition_date     = partition_date
        self.num_rows_per_writer = num_rows_per_writer

    def run(self) -> WriterResult:
        log.info(f"[{self.writer_id}] Pattern A — ghi vào partition {self.partition_date}")
        df    = self._make_payload(self.partition_date, self.num_rows_per_writer)
        entry = self.commit(df, "APPEND", target_key=self.partition_date)

        AUDIT_LOG.append(entry)
        rows_committed = self.num_rows_per_writer if entry.committed else 0
        return WriterResult(
            writer_id       = self.writer_id,
            rows_committed  = rows_committed,
            timestamp       = entry.timestamp,
            transaction_ids = [entry.transaction_id] if entry.committed else [],
        )


# ─── PATTERN B — OVERLAPPING WRITES ────────────────────────────────────────────

class OverlappingWriter(Writer):
    """
    Task 2.3: Pattern B — Overlapping Writes (True Conflicts).
    N writers cùng ghi vào 1 partition → xung đột thực sự, cần conflict resolution.
    """

    def __init__(self, writer_id: str, table_path: str,
                 shared_partition: str, num_rows_per_writer: int = 30,
                 conflict_ratio: float = 1.0):
        super().__init__(writer_id, table_path)
        self.shared_partition    = shared_partition
        self.num_rows_per_writer = num_rows_per_writer
        self.conflict_ratio      = conflict_ratio  # 1.0 = tất cả cùng partition

    def run(self) -> WriterResult:
        # conflict_ratio kiểm soát xác suất ghi vào partition bị tranh chấp
        if random.random() < self.conflict_ratio:
            target = self.shared_partition
        else:
            target = random.choice([p for p in ALL_PARTITIONS if p != self.shared_partition])

        log.info(f"[{self.writer_id}] Pattern B — ghi vào partition {target} "
                 f"(conflict_ratio={self.conflict_ratio})")
        df    = self._make_payload(target, self.num_rows_per_writer)
        entry = self.commit(df, "APPEND", target_key=target)

        AUDIT_LOG.append(entry)
        rows_committed = self.num_rows_per_writer if entry.committed else 0
        return WriterResult(
            writer_id       = self.writer_id,
            rows_committed  = rows_committed,
            timestamp       = entry.timestamp,
            transaction_ids = [entry.transaction_id] if entry.committed else [],
        )


# ─── PATTERN C — MAINTENANCE CONCURRENT ────────────────────────────────────────

class MaintenanceRunner(Writer):
    """
    Task 2.4 (Phần maintenance): Chạy OPTIMIZE / file compaction.
    """

    def run(self, stop_event: threading.Event) -> None:
        log.info(f"[{self.writer_id}] 🔧 Maintenance thread bắt đầu (OPTIMIZE loop)")
        count = 0
        while not stop_event.is_set():
            t_start = time.time()
            try:
                dt = DeltaTable(self.table_path)
                # Compact small files (Python deltalake API)
                dt.optimize.compact()
                latency_ms = (time.time() - t_start) * 1000
                count += 1
                log.info(f"[{self.writer_id}] ✅ OPTIMIZE #{count} — {latency_ms:.0f}ms")

                entry = AuditEntry(
                    transaction_id = str(uuid.uuid4()),
                    writer_id      = self.writer_id,
                    operation      = "OPTIMIZE",
                    target_key     = "all_partitions",
                    value          = {"optimize_run": count},
                    timestamp      = t_start,
                    committed      = True,
                    retry_count    = 0,
                    commit_version = dt.version(),
                    latency_ms     = latency_ms,
                )
                AUDIT_LOG.append(entry)
            except Exception as e:
                log.warning(f"[{self.writer_id}] ⚠️  OPTIMIZE fail — {e}")

            stop_event.wait(timeout=3.0)   # OPTIMIZE mỗi 3 giây

        log.info(f"[{self.writer_id}] 🔧 Maintenance thread dừng sau {count} lần OPTIMIZE")


class AppendWriter(Writer):
    """Task 2.4 (Phần append): Ghi liên tục trong khi Maintenance chạy."""

    def __init__(self, writer_id: str, table_path: str,
                 num_rows_per_writer: int = 20, num_rounds: int = 5):
        super().__init__(writer_id, table_path)
        self.num_rows_per_writer = num_rows_per_writer
        self.num_rounds          = num_rounds

    def run(self) -> WriterResult:
        total_committed = 0
        all_txn_ids     = []
        t_start_overall = time.time()

        for round_idx in range(self.num_rounds):
            partition = random.choice(ALL_PARTITIONS)
            df        = self._make_payload(partition, self.num_rows_per_writer)
            entry     = self.commit(df, "APPEND", target_key=partition)
            AUDIT_LOG.append(entry)

            if entry.committed:
                total_committed += self.num_rows_per_writer
                all_txn_ids.append(entry.transaction_id)

            time.sleep(random.uniform(0.1, 0.5))   # Ghi không đều

        return WriterResult(
            writer_id       = self.writer_id,
            rows_committed  = total_committed,
            timestamp       = t_start_overall,
            transaction_ids = all_txn_ids,
        )


# ─── GLOBAL AUDIT LOG ──────────────────────────────────────────────────────────
# Thread-safe list (GIL đủ bảo vệ cho append)
AUDIT_LOG: List[AuditEntry] = []
AUDIT_LOCK = threading.Lock()


# ─── WORKLOAD GENERATOR ────────────────────────────────────────────────────────

class WorkloadGenerator:
    """
    Task 2.5: Tham số điều chỉnh đầy đủ.
    Orchestrates tất cả 3 patterns.
    """

    def __init__(
        self,
        table_path:          str   = TABLE_PATH,
        num_writers:         int   = 4,
        num_rows_per_writer: int   = 50,
        conflict_ratio:      float = 1.0,
        pattern_type:        str   = "A",   # "A" | "B" | "C"
        results_dir:         str   = RESULTS_DIR,
    ):
        self.table_path          = table_path
        self.num_writers         = num_writers
        self.num_rows_per_writer = num_rows_per_writer
        self.conflict_ratio      = conflict_ratio
        self.pattern_type        = pattern_type.upper()
        self.results_dir         = results_dir
        os.makedirs(results_dir, exist_ok=True)

    # ── Pattern A ──────────────────────────────────────────────────────────────

    def run_pattern_a(self) -> List[WriterResult]:
        """Disjoint Writes: gán mỗi writer 1 partition riêng."""
        global AUDIT_LOG
        AUDIT_LOG = []
        log.info(f"▶ Pattern A — {self.num_writers} writers, disjoint partitions")

        # Đảm bảo đủ partition cho từng writer
        partitions = (ALL_PARTITIONS * ((self.num_writers // len(ALL_PARTITIONS)) + 1))
        partitions = partitions[:self.num_writers]

        writers = [
            DisjointWriter(
                writer_id           = f"writer_{i}",
                table_path          = self.table_path,
                partition_date      = partitions[i],
                num_rows_per_writer = self.num_rows_per_writer,
            )
            for i in range(self.num_writers)
        ]

        results = []
        with ThreadPoolExecutor(max_workers=self.num_writers,
                                thread_name_prefix="PatternA") as exe:
            futures = {exe.submit(w.run): w.writer_id for w in writers}
            for fut in as_completed(futures):
                results.append(fut.result())

        return results

    # ── Pattern B ──────────────────────────────────────────────────────────────

    def run_pattern_b(self) -> List[WriterResult]:
        """Overlapping Writes: N writers cùng ghi vào shared partition."""
        global AUDIT_LOG
        AUDIT_LOG = []
        log.info(f"▶ Pattern B — {self.num_writers} writers, "
                 f"conflict_ratio={self.conflict_ratio}")

        shared_partition = ALL_PARTITIONS[0]   # Tất cả tranh nhau 2024-01-01
        writers = [
            OverlappingWriter(
                writer_id           = f"writer_{i}",
                table_path          = self.table_path,
                shared_partition    = shared_partition,
                num_rows_per_writer = self.num_rows_per_writer,
                conflict_ratio      = self.conflict_ratio,
            )
            for i in range(self.num_writers)
        ]

        results = []
        with ThreadPoolExecutor(max_workers=self.num_writers,
                                thread_name_prefix="PatternB") as exe:
            futures = {exe.submit(w.run): w.writer_id for w in writers}
            for fut in as_completed(futures):
                results.append(fut.result())

        return results

    # ── Pattern C ──────────────────────────────────────────────────────────────

    def run_pattern_c(self) -> List[WriterResult]:
        """Maintenance Concurrent: OPTIMIZE + N Append writers song song."""
        global AUDIT_LOG
        AUDIT_LOG = []
        log.info(f"▶ Pattern C — {self.num_writers} append writers + 1 OPTIMIZE thread")

        stop_event = threading.Event()

        # Maintenance thread
        maintenance = MaintenanceRunner("maintenance_thread", self.table_path)
        maint_thread = threading.Thread(
            target=maintenance.run,
            args=(stop_event,),
            name="Maintenance",
            daemon=True,
        )
        maint_thread.start()

        # Append writers
        writers = [
            AppendWriter(
                writer_id           = f"writer_{i}",
                table_path          = self.table_path,
                num_rows_per_writer = self.num_rows_per_writer,
                num_rounds          = 5,
            )
            for i in range(self.num_writers)
        ]

        results = []
        with ThreadPoolExecutor(max_workers=self.num_writers,
                                thread_name_prefix="PatternC") as exe:
            futures = {exe.submit(w.run): w.writer_id for w in writers}
            for fut in as_completed(futures):
                results.append(fut.result())

        stop_event.set()
        maint_thread.join(timeout=5)
        return results

    # ── Main dispatcher ────────────────────────────────────────────────────────

    def run(self) -> Dict[str, Any]:
        """Chạy pattern được chọn và bàn giao kết quả."""
        t0 = time.time()
        dispatch = {
            "A": self.run_pattern_a,
            "B": self.run_pattern_b,
            "C": self.run_pattern_c,
        }
        if self.pattern_type not in dispatch:
            raise ValueError(f"pattern_type phải là A/B/C, nhận được: {self.pattern_type}")

        results: List[WriterResult] = dispatch[self.pattern_type]()
        elapsed = time.time() - t0

        # ── Tổng hợp metrics ──────────────────────────────────────────────────
        total_rows   = sum(r.rows_committed for r in results)
        committed_ok = sum(1 for e in AUDIT_LOG if e.committed)
        committed_fail = len(AUDIT_LOG) - committed_ok
        throughput   = total_rows / elapsed if elapsed > 0 else 0

        summary = {
            "pattern_type":       self.pattern_type,
            "num_writers":        self.num_writers,
            "num_rows_per_writer": self.num_rows_per_writer,
            "conflict_ratio":     self.conflict_ratio,
            "elapsed_seconds":    round(elapsed, 3),
            "total_rows_committed": total_rows,
            "throughput_rows_per_sec": round(throughput, 2),
            "commit_success_count":    committed_ok,
            "commit_fail_count":       committed_fail,
            "abort_rate":         round(committed_fail / max(len(AUDIT_LOG), 1), 4),
        }

        # ── Bàn giao cho Thành viên B (Task 2.6) ─────────────────────────────
        handoff = {
            "summary":    summary,
            "writer_results": [asdict(r) for r in results],
            "audit_log":  [asdict(e) for e in AUDIT_LOG],
        }
        ts        = int(time.time())
        out_path  = os.path.join(
            self.results_dir,
            f"commit_log_pattern{self.pattern_type}_{ts}.json"
        )
        with open(out_path, "w") as f:
            json.dump(handoff, f, indent=2)

        log.info(f"\n{'='*60}")
        log.info(f"Pattern {self.pattern_type} hoàn thành!")
        log.info(f"  Thời gian:     {elapsed:.2f}s")
        log.info(f"  Rows committed: {total_rows}")
        log.info(f"  Throughput:    {throughput:.1f} rows/s")
        log.info(f"  Abort rate:    {summary['abort_rate']:.1%}")
        log.info(f"  Bàn giao B:   {out_path}")
        log.info(f"{'='*60}\n")

        return handoff


# ─── DEMO ──────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys

    # Kiểm tra bảng tồn tại
    if not os.path.exists(os.path.join(TABLE_PATH, "_delta_log")):
        print("⚠️  Bảng chưa tồn tại. Chạy setup_environment.py trước!")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("🏭 WORKLOAD GENERATOR — Demo 3 Patterns")
    print("=" * 60)

    for pattern in ["A", "B", "C"]:
        print(f"\n{'─'*60}")
        print(f"  Chạy Pattern {pattern}...")
        print(f"{'─'*60}")
        gen = WorkloadGenerator(
            num_writers         = 4,
            num_rows_per_writer = 20,
            conflict_ratio      = 1.0,
            pattern_type        = pattern,
        )
        gen.run()

    print("\n✅ Demo hoàn tất! Xem kết quả trong ./results/")
