"""
test_experiment_runner.py
=========================
Vai trò: kiểm tra phần đo đạc của src/experiment_runner.py bằng dữ liệu giả:
  - run sạch  -> throughput "đúng" > 0
  - run có lost update -> throughput "đúng" bị ép về 0 (luật của đề)
  - summarize() đếm đúng số run bị sai

Chạy: pytest tests/test_experiment_runner.py -v
"""

import pandas as pd

from src.experiment_runner import evaluate_run, fake_run_once, run_experiment, summarize


def test_clean_run_has_positive_correct_throughput():
    # run_idx=0 -> fake_run_once KHÔNG cố ý làm mất dữ liệu
    record = evaluate_run(fake_run_once("B", "Baseline", 4, run_idx=0))
    assert record["passed"] is True
    assert record["correct_commits_per_sec"] > 0
    assert record["correct_commits_per_sec"] == record["commits_per_sec"]


def test_lost_update_forces_correct_throughput_to_zero():
    # run_idx=3 -> fake_run_once cố ý bỏ dữ liệu của 1 giao dịch đã commit
    record = evaluate_run(fake_run_once("B", "Baseline", 4, run_idx=3))
    assert record["LostUpdateCount"] >= 1
    assert record["passed"] is False
    assert record["correct_commits_per_sec"] == 0.0
    assert record["commits_per_sec"] > 0          # thông lượng thô vẫn > 0, chỉ bản "đúng" bị ép về 0


def test_run_experiment_returns_one_row_per_run_with_required_columns():
    runs = run_experiment("A", "Baseline", 2, num_runs=4, run_once=fake_run_once, source="synthetic")
    assert len(runs) == 4
    for column in ["source", "pattern", "strategy", "num_writers", "run_idx",
                   "correct_commits_per_sec", "p95_latency_ms", "LostUpdateCount", "passed"]:
        assert column in runs.columns
    assert set(runs["source"]) == {"synthetic"}


def test_summarize_counts_failed_runs():
    runs = run_experiment("A", "Baseline", 2, num_runs=4, run_once=fake_run_once, source="synthetic")
    summary = summarize(runs)
    assert len(summary) == 1
    assert summary.loc[0, "runs"] == 4
    assert summary.loc[0, "failed_runs"] == 1     # chỉ run_idx=3 bị cố ý làm sai
    assert isinstance(summary, pd.DataFrame)
