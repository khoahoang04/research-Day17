"""
oracle.py
=========
Vai trò của file: CORRECTNESS ORACLE (phần của Thành viên B).

Oracle trả lời 2 câu hỏi, HOÀN TOÀN ĐỘC LẬP với Delta Lake:
  1. compute_ground_truth(): "Bảng ĐÁNG LẼ phải có gì?"
       -> tính từ audit log (các giao dịch writer báo là đã commit)
  2. validate():              "Bảng THẬT khác đáp án ở đâu?"
       -> so bảng thật với đáp án, đếm lỗi

Vì sao cần oracle? Job báo "success" vẫn có thể đã làm mất/trùng dữ liệu
(ví dụ lost update). Chỉ có so với một đáp án tính độc lập mới biết được.

Ba loại lỗi được đếm (đúng tên trong đề):
  LostUpdateCount : giao dịch đã commit nhưng dữ liệu KHÔNG có trong bảng
  WrongFinalRows  : dòng trong bảng mà không giao dịch nào đã commit tạo ra
  DuplicateRows   : cùng một id xuất hiện nhiều hơn 1 lần
Thêm: AmountMismatch (dòng đủ nhưng tổng amount sai -> "sai giá trị").

Hai loại thao tác được hiểu (đọc từ audit log, khớp data/audit_log_schema.json):
  - APPEND : value = {"num_rows", "sum_amount", "ids_range": "100–119"}
             (hoặc value["ids"] = [danh sách id] nếu A ghi chi tiết hơn)
  - Bộ đếm : value = {"key": ..., "delta": ...}   <- ĐỀ XUẤT với Thành viên A,
             dùng để tạo ra lost update thật (đọc-tính-ghi).
  - OPTIMIZE và các thao tác khác: không đổi dữ liệu logic -> bỏ qua.
"""

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set

import pandas as pd

log = logging.getLogger(__name__)

# Thao tác APPEND: dữ liệu mới được thêm vào bảng
APPEND_OPS = {"APPEND"}

# "ids_range" do workload_generator ghi dạng "100000–100019" (dấu gạch ngang dài "–").
# Regex chấp nhận cả "–", "—" và "-".
_RANGE_RE = re.compile(r"(\d+)\s*[–—-]\s*(\d+)")


# ─── KIỂU DỮ LIỆU ──────────────────────────────────────────────────────────────

@dataclass
class GroundTruth:
    """
    Đáp án đúng, do compute_ground_truth() tạo ra.
    (Kế hoạch ghi 'trả về DataFrame'; ở đây gói DataFrame + vài thông tin phụ
    vào 1 object để validate() dùng, DataFrame nằm ở `.rows`.)
    """
    rows: pd.DataFrame                      # các dòng KỲ VỌNG có trong bảng: id, transaction_id, partition_date
    expected_amount_sum: float              # tổng sum_amount của các APPEND đã commit
    counters: Dict[str, float]              # key -> giá trị bộ đếm kỳ vọng
    committed_txn_ids: List[str]            # các giao dịch GHI DỮ LIỆU đã commit (đã khử trùng)
    initial_ids: Set[int]                   # id đã có sẵn trong bảng TRƯỚC khi chạy
    duplicate_txn_in_log: int = 0           # số lần cùng transaction_id xuất hiện lặp trong log
    unverifiable_txn_ids: List[str] = field(default_factory=list)  # APPEND không suy ra được id
    warnings: List[str] = field(default_factory=list)


# ─── HÀM PHỤ ───────────────────────────────────────────────────────────────────

def _is_counter_value(value: Any) -> bool:
    """Thao tác bộ đếm có dạng {"key": ..., "delta": ...}."""
    return isinstance(value, dict) and "key" in value and "delta" in value


def _expected_ids(entry: Dict[str, Any]) -> Optional[List[int]]:
    """
    Suy ra danh sách id mà 1 giao dịch APPEND đã ghi.
    Ưu tiên value["ids"] (chi tiết); nếu không có thì đọc value["ids_range"].
    Trả về None nếu không suy ra được (để báo là 'không kiểm chứng được').
    """
    value = entry.get("value") or {}

    if "ids" in value:
        return [int(i) for i in value["ids"]]

    match = _RANGE_RE.search(str(value.get("ids_range", "")))
    if not match:
        return None

    low, high = int(match.group(1)), int(match.group(2))
    ids = list(range(low, high + 1))

    # Dải id chỉ đáng tin nếu id liên tục: số id phải bằng num_rows
    num_rows = value.get("num_rows")
    if num_rows is not None and int(num_rows) != len(ids):
        return None
    return ids


def load_audit_log(path: str) -> List[Dict[str, Any]]:
    """
    Đọc file JSON do workload_generator bàn giao (results/commit_log_*.json).
    File có thể là dict {"audit_log": [...]} hoặc trực tiếp là list.
    """
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data["audit_log"] if isinstance(data, dict) else data


def read_table_state(table_path: str) -> pd.DataFrame:
    """Đọc trạng thái THẬT của bảng Delta (import trễ để test oracle không cần deltalake)."""
    from deltalake import DeltaTable
    return DeltaTable(table_path).to_pandas()


def snapshot_initial_ids(table_state: pd.DataFrame) -> Set[int]:
    """
    Chụp các id đang có TRƯỚC khi chạy experiment.
    Gọi hàm này ngay trước khi cho các writer chạy, rồi đưa kết quả vào
    compute_ground_truth(initial_ids=...) để oracle không coi dữ liệu cũ là 'dòng thừa'.
    """
    return {int(i) for i in table_state["id"]}


# ─── 1) ĐÁP ÁN ĐÚNG ────────────────────────────────────────────────────────────

def compute_ground_truth(
    audit_log: Iterable[Dict[str, Any]],
    initial_ids: Optional[Iterable[int]] = None,
    initial_counters: Optional[Dict[str, float]] = None,
) -> GroundTruth:
    """
    Tính trạng thái đúng tuyệt đối từ audit log:
      - chỉ lấy giao dịch committed=True
      - sắp theo commit_version tăng dần (KHÔNG dùng timestamp: đó chỉ là giờ bắt đầu
        thử commit, không phải thứ tự thật trong log). Với APPEND và cộng bộ đếm thì
        thứ tự không đổi kết quả, nhưng giữ sắp xếp để dùng được cho thao tác ghi đè sau này.
      - khử trùng theo transaction_id (retry không được làm tăng số lần tính)
    """
    committed = [e for e in audit_log if e.get("committed")]
    committed.sort(key=lambda e: (
        e.get("commit_version") is None,       # version None xếp cuối
        e.get("commit_version") or 0,
        e.get("timestamp") or 0.0,
    ))

    seen: Set[str] = set()
    duplicate_in_log = 0
    rows: List[Dict[str, Any]] = []
    amount_sum = 0.0
    counters: Dict[str, float] = dict(initial_counters or {})
    committed_txn_ids: List[str] = []
    unverifiable: List[str] = []
    warnings: List[str] = []

    for entry in committed:
        txn_id = entry["transaction_id"]
        if txn_id in seen:                      # cùng 1 giao dịch bị log nhiều lần
            duplicate_in_log += 1
            continue
        seen.add(txn_id)

        value = entry.get("value") or {}

        if _is_counter_value(value):            # cộng bộ đếm
            key = str(value["key"])
            counters[key] = counters.get(key, 0) + value["delta"]
            committed_txn_ids.append(txn_id)
            continue

        if entry.get("operation") not in APPEND_OPS:   # OPTIMIZE,... không đổi dữ liệu logic
            continue

        ids = _expected_ids(entry)
        if ids is None:
            unverifiable.append(txn_id)
            continue

        partition = entry.get("target_key", "")
        rows.extend({"id": i, "transaction_id": txn_id, "partition_date": partition} for i in ids)
        amount_sum += float(value.get("sum_amount") or 0.0)
        committed_txn_ids.append(txn_id)

    rows_df = pd.DataFrame(rows, columns=["id", "transaction_id", "partition_date"])
    initial = {int(i) for i in (initial_ids or [])}

    # Cảnh báo lỗi của CHÍNH workload (không phải lỗi hệ thống) để khỏi hiểu nhầm kết quả
    if rows_df["id"].duplicated().any():
        warnings.append("Workload tạo id trùng giữa các giao dịch đã commit -> DuplicateRows có thể là lỗi dữ liệu test.")
    if initial and set(rows_df["id"]) & initial:
        warnings.append("Có id của giao dịch mới trùng với id đã có sẵn trong bảng.")
    if unverifiable:
        warnings.append(f"{len(unverifiable)} giao dịch APPEND không suy ra được id (bỏ qua khi so).")

    return GroundTruth(
        rows=rows_df,
        expected_amount_sum=round(amount_sum, 2),
        counters=counters,
        committed_txn_ids=committed_txn_ids,
        initial_ids=initial,
        duplicate_txn_in_log=duplicate_in_log,
        unverifiable_txn_ids=unverifiable,
        warnings=warnings,
    )


# ─── 2) SO BẢNG THẬT VỚI ĐÁP ÁN ────────────────────────────────────────────────

def validate(
    table_state: pd.DataFrame,
    ground_truth: GroundTruth,
    counter_state: Optional[Dict[str, float]] = None,
    amount_tolerance_per_txn: float = 0.01,
) -> Dict[str, Any]:
    """
    So bảng thật (table_state, cần cột 'id', tùy chọn 'amount') với đáp án.
    counter_state: dict key -> giá trị bộ đếm thật trong bảng (None = không kiểm bộ đếm).

    Trả về dict có LostUpdateCount, WrongFinalRows, DuplicateRows, AmountMismatch, passed.
    """
    gt = ground_truth

    counts = table_state["id"].value_counts()               # id -> số lần xuất hiện
    actual_ids = {int(i) for i in counts.index}
    expected_ids = {int(i) for i in gt.rows["id"]}

    # (1) LostUpdateCount: giao dịch đã commit nhưng thiếu ít nhất 1 id trong bảng
    missing_ids = expected_ids - actual_ids
    lost_txn_ids = sorted(
        gt.rows.loc[gt.rows["id"].isin(missing_ids), "transaction_id"].unique()
    )
    lost = len(lost_txn_ids)

    # (2) DuplicateRows: số bản sao THỪA (id xuất hiện k lần -> thừa k-1)
    duplicate_rows = int((counts - 1).clip(lower=0).sum())

    # (3) WrongFinalRows: dòng không do giao dịch đã commit nào tạo ra (và không phải dữ liệu cũ)
    #     Ví dụ: giao dịch bị log là thất bại nhưng thực ra đã ghi vào bảng.
    extra_ids = actual_ids - expected_ids - gt.initial_ids
    wrong_rows = int(counts.loc[list(extra_ids)].sum()) if extra_ids else 0

    # (4) Bộ đếm (nếu có): thấp hơn kỳ vọng = mất cập nhật, cao hơn = cộng thừa/trùng
    counter_issues: Dict[str, Dict[str, float]] = {}
    if counter_state is not None:
        for key, expected in gt.counters.items():
            actual = counter_state.get(key, 0)
            if abs(actual - expected) < 1e-9:
                continue
            counter_issues[key] = {"expected": expected, "actual": actual}
            if actual < expected:
                lost += 1
            else:
                wrong_rows += 1

    # (5) "Sai giá trị": các dòng đủ id, không trùng, không thừa nhưng tổng amount lệch.
    #     Chỉ xét khi các lỗi trên đều bằng 0 để không đếm đôi cùng một lỗi.
    amount_diff = None
    amount_mismatch = False
    if "amount" in table_state.columns and expected_ids:
        new_rows = table_state[table_state["id"].isin(expected_ids)]
        amount_diff = round(float(new_rows["amount"].sum()) - gt.expected_amount_sum, 2)
        ids_ok = (lost == 0 and wrong_rows == 0 and duplicate_rows == 0)
        tolerance = amount_tolerance_per_txn * max(1, len(gt.committed_txn_ids))
        amount_mismatch = ids_ok and abs(amount_diff) > tolerance

    passed = (lost == 0 and wrong_rows == 0 and duplicate_rows == 0 and not amount_mismatch)

    return {
        "LostUpdateCount": lost,
        "WrongFinalRows": wrong_rows,
        "DuplicateRows": duplicate_rows,
        "AmountMismatch": amount_mismatch,
        "passed": passed,
        "details": {
            "missing_id_count": len(missing_ids),
            "extra_id_count": len(extra_ids),
            "lost_txn_ids": lost_txn_ids,
            "counter_issues": counter_issues,
            "amount_diff": amount_diff,
            "duplicate_txn_in_log": gt.duplicate_txn_in_log,
            "warnings": gt.warnings,
        },
    }
