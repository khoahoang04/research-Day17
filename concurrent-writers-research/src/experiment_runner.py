"""
experiment_runner.py
====================
Vai trò của file: ĐIỀU PHỐI THÍ NGHIỆM (phần của Thành viên B).

Với mỗi cấu hình (pattern, strategy, num_writers) file này:
  1. chạy `num_runs` lần (lỗi concurrency xảy ra ngẫu nhiên, 1 lần không chứng minh gì)
  2. sau mỗi lần chạy, gọi oracle để kiểm tra dữ liệu có đúng không
  3. tính throughput, p50/p95 latency, conflict rate...
  4. NẾU oracle báo sai (LostUpdate/Duplicate/Wrong...) -> throughput "đúng" bị ép về 0
     (luật của đề: run sai dữ liệu thì không được tính điểm)
  5. xuất CSV (mỗi dòng = 1 lần chạy) + bảng tổng hợp (mỗi dòng = 1 cấu hình)

File này KHÔNG tự biết cách chạy workload thật. Nó nhận vào một hàm `run_once`
(do ta nối với code của Thành viên A sau) trả về `RunOutput`. Nhờ vậy phần phân tích
của B chạy và test được ngay cả khi A chưa xong.

Chạy thử bằng dữ liệu GIẢ (chỉ để kiểm tra pipeline, KHÔNG phải kết quả nghiên cứu):
    python -m src.experiment_runner --demo --out results/demo_synthetic_runs.csv
"""

import argparse
import logging
import os
import random
import uuid
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Set

import numpy as np
import pandas as pd

from src.oracle import compute_ground_truth, validate

log = logging.getLogger(__name__)


# ─── KIỂU DỮ LIỆU ──────────────────────────────────────────────────────────────

@dataclass
class RunOutput:
    """
    Mọi thứ cần có sau MỘT lần chạy để oracle và phần đo đạc làm việc.
    Hàm `run_once` (nối với code của A) phải trả về đúng kiểu này.
    """
    elapsed_seconds: float                      # tổng thời gian cả lần chạy
    audit_log: List[Dict[str, Any]]             # audit log (list dict) theo data/audit_log_schema.json
    table_state: pd.DataFrame                   # bảng THẬT sau khi chạy xong (cột id, amount,...)
    initial_ids: Set[int]                       # id có sẵn TRƯỚC khi chạy (snapshot_initial_ids)
    commit_latencies_ms: List[float]            # latency của từng giao dịch
    abort_count: int = 0                        # số giao dịch cuối cùng thất bại
    retry_count: int = 0                        # tổng số lần retry
    attempts: Optional[int] = None              # tổng số giao dịch (mặc định = số latency)
    counter_state: Optional[Dict[str, float]] = None      # bộ đếm thật (nếu có pattern bộ đếm)
    initial_counters: Optional[Dict[str, float]] = None   # bộ đếm trước khi chạy


# Chữ ký của hàm chạy 1 lần: (pattern, strategy, num_writers, run_idx) -> RunOutput
RunOnce = Callable[[str, str, int, int], RunOutput]


# ─── ĐÁNH GIÁ 1 LẦN CHẠY ───────────────────────────────────────────────────────

def evaluate_run(out: RunOutput) -> Dict[str, Any]:
    """Gọi oracle + tính các chỉ số cho 1 lần chạy. Trả về 1 dict (= 1 dòng CSV)."""
    ground_truth = compute_ground_truth(out.audit_log, out.initial_ids, out.initial_counters)
    verdict = validate(out.table_state, ground_truth, out.counter_state)

    elapsed = max(out.elapsed_seconds, 1e-9)
    commits = len(ground_truth.committed_txn_ids)      # số giao dịch thành công (đã khử trùng)
    rows = len(ground_truth.rows)                      # số dòng thành công
    attempts = out.attempts if out.attempts is not None else len(out.commit_latencies_ms)

    latencies = np.asarray(out.commit_latencies_ms, dtype=float)
    p50 = float(np.percentile(latencies, 50)) if latencies.size else 0.0
    p95 = float(np.percentile(latencies, 95)) if latencies.size else 0.0

    passed = bool(verdict["passed"])
    commits_per_sec = commits / elapsed
    rows_per_sec = rows / elapsed

    return {
        "elapsed_seconds": round(elapsed, 4),
        "commits": commits,
        "rows": rows,
        # Thông lượng thô (chưa xét đúng/sai)
        "commits_per_sec": round(commits_per_sec, 3),
        "rows_per_sec": round(rows_per_sec, 3),
        # Thông lượng ĐÚNG: ép về 0 nếu oracle báo sai (đây là chỉ số chính của đề)
        "correct_commits_per_sec": round(commits_per_sec, 3) if passed else 0.0,
        "correct_rows_per_sec": round(rows_per_sec, 3) if passed else 0.0,
        "p50_latency_ms": round(p50, 2),
        "p95_latency_ms": round(p95, 2),
        "attempts": attempts,
        "abort_count": out.abort_count,
        "retry_count": out.retry_count,
        "conflict_rate": round(out.abort_count / max(attempts, 1), 4),
        "LostUpdateCount": verdict["LostUpdateCount"],
        "WrongFinalRows": verdict["WrongFinalRows"],
        "DuplicateRows": verdict["DuplicateRows"],
        "AmountMismatch": verdict["AmountMismatch"],
        "passed": passed,
    }


# ─── CHẠY NHIỀU LẦN ────────────────────────────────────────────────────────────

def run_experiment(
    pattern: str,
    strategy: str,
    num_writers: int,
    num_runs: int,
    run_once: RunOnce,
    source: str = "real",
) -> pd.DataFrame:
    """
    Chạy cùng 1 cấu hình `num_runs` lần (khuyến nghị 5–10). Trả về DataFrame, mỗi dòng 1 lần chạy.
    source: "real" cho kết quả thật, "synthetic" cho dữ liệu giả — cột này để biểu đồ
            không bao giờ nhầm số giả với số thật.
    """
    records = []
    for run_idx in range(num_runs):
        out = run_once(pattern, strategy, num_writers, run_idx)
        record = evaluate_run(out)
        record.update({
            "source": source,
            "pattern": pattern,
            "strategy": strategy,
            "num_writers": num_writers,
            "run_idx": run_idx,
        })
        records.append(record)
        log.info(f"[{pattern}/{strategy}/{num_writers}w run {run_idx}] "
                 f"correct_commits/s={record['correct_commits_per_sec']} passed={record['passed']}")
    return pd.DataFrame(records)


# ─── TỔNG HỢP ──────────────────────────────────────────────────────────────────

def summarize(runs: pd.DataFrame) -> pd.DataFrame:
    """
    Gộp các lần chạy theo cấu hình: không chỉ trung bình mà có p50, p95, stddev
    (đề yêu cầu báo phân phối và p95, không chỉ trung bình).
    """
    keys = ["source", "pattern", "strategy", "num_writers"]
    grouped = runs.groupby(keys)
    summary = grouped.agg(
        runs=("run_idx", "count"),
        failed_runs=("passed", lambda s: int((~s.astype(bool)).sum())),   # số lần oracle báo sai
        tput_mean=("correct_commits_per_sec", "mean"),
        tput_p50=("correct_commits_per_sec", "median"),
        tput_p95=("correct_commits_per_sec", lambda s: s.quantile(0.95)),
        tput_std=("correct_commits_per_sec", "std"),
        p95_latency_mean=("p95_latency_ms", "mean"),
        conflict_rate_mean=("conflict_rate", "mean"),
        retries_mean=("retry_count", "mean"),
    ).reset_index()
    return summary.round(3)


def save_results(runs: pd.DataFrame, out_path: str) -> str:
    """Ghi CSV từng lần chạy và CSV tổng hợp (cùng tên + '_summary'). Trả về đường dẫn tổng hợp."""
    os.makedirs(os.path.dirname(os.path.abspath(out_path)), exist_ok=True)
    runs.to_csv(out_path, index=False)
    summary_path = out_path.replace(".csv", "_summary.csv")
    summarize(runs).to_csv(summary_path, index=False)
    return summary_path


# ─── DỮ LIỆU GIẢ ĐỂ THỬ PIPELINE ───────────────────────────────────────────────

def fake_run_once(pattern: str, strategy: str, num_writers: int, run_idx: int) -> RunOutput:
    """
    Tạo RunOutput GIẢ, ngẫu nhiên có seed, chỉ để kiểm tra pipeline (oracle -> CSV -> biểu đồ).
    Các con số KHÔNG mang ý nghĩa nghiên cứu. Cứ 4 lần chạy có 1 lần cố ý làm mất dữ liệu
    để xem oracle ép throughput về 0.
    """
    rng = random.Random(f"{pattern}|{strategy}|{num_writers}|{run_idx}")
    initial_ids = set(range(1000))
    audit_log, table_rows, latencies = [], [{"id": i, "amount": 1.0} for i in initial_ids], []
    version, aborts = 0, 0

    committed_entries = []
    for w in range(num_writers):
        for r in range(5):                                       # mỗi writer ghi 5 giao dịch
            start = 1_000_000 + w * 10_000 + r * 100             # id không bao giờ trùng nhau
            ids = list(range(start, start + 10))
            amounts = [round(rng.uniform(10, 1000), 2) for _ in ids]
            ok = rng.random() > 0.1                              # 10% giao dịch thất bại
            latency = rng.uniform(5, 50) * (1 + num_writers / 8)
            latencies.append(latency)
            if ok:
                version += 1
            else:
                aborts += 1
            entry = {
                "transaction_id": str(uuid.UUID(int=rng.getrandbits(128))),
                "writer_id": f"writer_{w}",
                "operation": "APPEND",
                "target_key": "2024-01-01",
                "value": {"num_rows": len(ids), "sum_amount": round(sum(amounts), 2),
                          "ids_range": f"{ids[0]}–{ids[-1]}"},
                "timestamp": float(len(audit_log)),
                "committed": ok,
                "retry_count": 0,
                "commit_version": version if ok else None,
                "latency_ms": latency,
            }
            audit_log.append(entry)
            if ok:
                committed_entries.append((entry, ids, amounts))
                table_rows.extend({"id": i, "amount": a} for i, a in zip(ids, amounts))

    # Cố ý mất dữ liệu: bỏ các dòng của 1 giao dịch đã commit (mô phỏng lost update)
    if run_idx % 4 == 3 and committed_entries:
        _, lost_ids, _ = committed_entries[0]
        table_rows = [row for row in table_rows if row["id"] not in set(lost_ids)]

    return RunOutput(
        elapsed_seconds=sum(latencies) / max(num_writers, 1) / 1000 + 0.05,
        audit_log=audit_log,
        table_state=pd.DataFrame(table_rows),
        initial_ids=initial_ids,
        commit_latencies_ms=latencies,
        abort_count=aborts,
        retry_count=0,
    )


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Chạy thí nghiệm nhiều lần và xuất CSV.")
    parser.add_argument("--demo", action="store_true", help="dùng dữ liệu GIẢ để thử pipeline")
    parser.add_argument("--out", default="results/experiment_results.csv", help="đường dẫn CSV đầu ra")
    parser.add_argument("--runs", type=int, default=8, help="số lần chạy mỗi cấu hình")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if not args.demo:
        raise SystemExit(
            "Chưa nối với code của Thành viên A. Sau khi A push bản cuối, viết hàm run_once "
            "(trả về RunOutput) rồi gọi run_experiment(...). Tạm thời dùng --demo để thử pipeline."
        )

    frames = []
    for pattern in ["A", "B", "C"]:
        for strategy in ["Baseline", "ExponentialBackoff+Jitter", "SelectiveSerialization"]:
            for num_writers in [2, 4, 8]:
                frames.append(run_experiment(pattern, strategy, num_writers, args.runs,
                                             fake_run_once, source="synthetic"))
    runs = pd.concat(frames, ignore_index=True)
    summary_path = save_results(runs, args.out)

    print("\n⚠️  DỮ LIỆU GIẢ (synthetic) — chỉ để kiểm tra pipeline, KHÔNG dùng làm kết quả báo cáo.")
    print(f"✅ CSV từng lần chạy: {args.out}")
    print(f"✅ CSV tổng hợp:      {summary_path}")
    print(f"   Số lần chạy bị oracle báo sai: {int((~runs['passed']).sum())}/{len(runs)}")


if __name__ == "__main__":
    main()
