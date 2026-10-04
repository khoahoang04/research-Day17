"""
test_oracle.py
==============
Vai trò: unit test cho src/oracle.py bằng DỮ LIỆU GIẢ (không cần Delta Lake, không cần A).
Mỗi test dựng 1 audit log nhỏ + 1 bảng "thật" giả, rồi kiểm tra oracle bắt đúng lỗi.

Chạy (từ thư mục concurrent-writers-research):
    pytest tests/test_oracle.py -v
"""

import json

import pandas as pd

from src.oracle import (
    compute_ground_truth,
    load_audit_log,
    snapshot_initial_ids,
    validate,
)


# ─── HÀM DỰNG DỮ LIỆU GIẢ ──────────────────────────────────────────────────────

def make_append(txn_id, start, n=3, amount=10.0, committed=True, version=1, partition="2024-01-01"):
    """1 dòng audit log kiểu APPEND, ghi n dòng id = start..start+n-1, mỗi dòng `amount`."""
    return {
        "transaction_id": txn_id,
        "writer_id": "writer_0",
        "operation": "APPEND",
        "target_key": partition,
        "value": {
            "num_rows": n,
            "sum_amount": round(n * amount, 2),
            "ids_range": f"{start}–{start + n - 1}",     # dấu gạch ngang dài, y như A ghi
        },
        "timestamp": 1000.0 + (version or 0),
        "committed": committed,
        "retry_count": 0,
        "commit_version": version if committed else None,
        "latency_ms": 5.0,
    }


def make_table(entries, initial_n=5, amount=10.0, drop_txn=None, extra_rows=None):
    """
    Dựng bảng 'thật' giả từ các APPEND đã commit.
    drop_txn: bỏ hết dòng của giao dịch này (mô phỏng lost update).
    extra_rows: danh sách dict dòng thêm vào (mô phỏng trùng/thừa).
    """
    rows = [{"id": i, "amount": 1.0} for i in range(initial_n)]     # dữ liệu cũ: id 0..initial_n-1
    for e in entries:
        if not e["committed"] or e["transaction_id"] == drop_txn:
            continue
        value = e["value"]
        low, high = (int(x) for x in value["ids_range"].split("–"))
        rows.extend({"id": i, "amount": amount} for i in range(low, high + 1))
    rows.extend(extra_rows or [])
    return pd.DataFrame(rows)


def run_oracle(entries, table, **kwargs):
    """Chụp id cũ từ bảng 'trước khi chạy' rồi chạy oracle. Trả về dict kết quả."""
    initial_ids = set(range(5))
    gt = compute_ground_truth(entries, initial_ids=initial_ids)
    return validate(table, gt, **kwargs)


# ─── CÁC CA THỬ ────────────────────────────────────────────────────────────────

def test_clean_run_passes():
    entries = [make_append("t1", 100, version=1), make_append("t2", 200, version=2)]
    result = run_oracle(entries, make_table(entries))
    assert result["passed"] is True
    assert result["LostUpdateCount"] == 0
    assert result["WrongFinalRows"] == 0
    assert result["DuplicateRows"] == 0


def test_lost_update_is_detected():
    # Audit log nói t2 đã commit nhưng bảng không có dòng nào của t2
    entries = [make_append("t1", 100, version=1), make_append("t2", 200, version=2)]
    result = run_oracle(entries, make_table(entries, drop_txn="t2"))
    assert result["LostUpdateCount"] == 1
    assert result["details"]["lost_txn_ids"] == ["t2"]
    assert result["passed"] is False


def test_duplicate_rows_are_detected():
    # id 100 xuất hiện 2 lần trong bảng (ví dụ retry ghi trùng)
    entries = [make_append("t1", 100, version=1)]
    table = make_table(entries, extra_rows=[{"id": 100, "amount": 10.0}])
    result = run_oracle(entries, table)
    assert result["DuplicateRows"] == 1
    assert result["passed"] is False


def test_phantom_rows_from_uncommitted_txn_are_wrong_rows():
    # t2 bị log là thất bại (committed=False) nhưng dòng của nó vẫn nằm trong bảng
    t1 = make_append("t1", 100, version=1)
    t2 = make_append("t2", 200, committed=False, version=None)
    table = make_table([t1], extra_rows=[{"id": i, "amount": 10.0} for i in (200, 201, 202)])
    result = run_oracle([t1, t2], table)
    assert result["WrongFinalRows"] == 3
    assert result["passed"] is False


def test_wrong_value_is_detected_when_ids_are_fine():
    # Đủ id, không trùng, nhưng amount bị ghi sai (5.0 thay vì 10.0)
    entries = [make_append("t1", 100, version=1)]
    table = make_table(entries, amount=5.0)
    result = run_oracle(entries, table)
    assert result["LostUpdateCount"] == 0
    assert result["AmountMismatch"] is True
    assert result["passed"] is False


def test_duplicate_txn_id_in_log_is_counted_once():
    # Cùng transaction_id bị log 2 lần (retry): chỉ tính 1 lần, không sinh "dòng thiếu"
    e = make_append("t1", 100, version=1)
    entries = [e, dict(e)]
    gt = compute_ground_truth(entries, initial_ids=range(5))
    assert gt.duplicate_txn_in_log == 1
    assert len(gt.rows) == 3
    assert validate(make_table([e]), gt)["passed"] is True


def test_optimize_entries_are_ignored():
    # OPTIMIZE không đổi dữ liệu logic -> không tạo dòng kỳ vọng nào
    optimize = {
        "transaction_id": "m1", "writer_id": "maintenance_thread", "operation": "OPTIMIZE",
        "target_key": "all_partitions", "value": {"optimize_run": 1}, "timestamp": 1.0,
        "committed": True, "retry_count": 0, "commit_version": 3, "latency_ms": 1.0,
    }
    t1 = make_append("t1", 100, version=1)
    gt = compute_ground_truth([t1, optimize], initial_ids=range(5))
    assert len(gt.rows) == 3
    assert gt.committed_txn_ids == ["t1"]


def test_ground_truth_sorted_by_commit_version():
    # Log đưa vào lộn thứ tự: oracle phải sắp theo commit_version
    a = make_append("a", 100, version=5)
    b = make_append("b", 200, version=2)
    gt = compute_ground_truth([a, b], initial_ids=range(5))
    assert gt.committed_txn_ids == ["b", "a"]


def test_counter_lost_update_is_detected():
    # 4 lần cộng 1 vào key "7" đã commit -> kỳ vọng 4. Bảng chỉ có 3 = mất 1 lần cộng.
    entries = [
        {"transaction_id": f"c{i}", "writer_id": f"writer_{i}", "operation": "UPDATE",
         "target_key": "7", "value": {"key": "7", "delta": 1}, "timestamp": float(i),
         "committed": True, "retry_count": 0, "commit_version": i + 1, "latency_ms": 1.0}
        for i in range(4)
    ]
    gt = compute_ground_truth(entries, initial_ids=[], initial_counters={"7": 0})
    assert gt.counters["7"] == 4

    bad = validate(pd.DataFrame({"id": []}), gt, counter_state={"7": 3})
    assert bad["LostUpdateCount"] == 1
    assert bad["details"]["counter_issues"]["7"] == {"expected": 4, "actual": 3}
    assert bad["passed"] is False

    good = validate(pd.DataFrame({"id": []}), gt, counter_state={"7": 4})
    assert good["passed"] is True


def test_load_audit_log_reads_handoff_file(tmp_path):
    # File bàn giao của A có dạng {"summary":..., "audit_log":[...]}
    entries = [make_append("t1", 100, version=1)]
    path = tmp_path / "commit_log.json"
    path.write_text(json.dumps({"summary": {}, "audit_log": entries}), encoding="utf-8")
    assert load_audit_log(str(path)) == entries


def test_snapshot_initial_ids():
    table = pd.DataFrame({"id": [0, 1, 2]})
    assert snapshot_initial_ids(table) == {0, 1, 2}
