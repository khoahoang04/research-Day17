"""
setup_environment.py
====================
Task 1: Setup môi trường & tạo bảng Delta Lake mẫu

Chạy: python setup_environment.py

Output:
  - Bảng Delta Lake tại ./data/transactions/
  - Audit log schema in ./data/audit_log_schema.json
  - Xác nhận môi trường sẵn sàng
"""

import os
import json
import time
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from datetime import datetime, timedelta
import random


# ─── CONFIG ────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TABLE_PATH = os.path.join(BASE_DIR, "data", "transactions")
AUDIT_SCHEMA_PATH = os.path.join(BASE_DIR, "data", "audit_log_schema.json")
NUM_INITIAL_ROWS = 1000
NUM_PARTITIONS = 8  # 8 ngày khác nhau


# ─── SCHEMA ────────────────────────────────────────────────────────────────────
TRANSACTIONS_SCHEMA = pa.schema([
    pa.field("id",             pa.int64(),   nullable=False),   # PK
    pa.field("user_id",        pa.int64(),   nullable=False),
    pa.field("amount",         pa.float64(), nullable=False),
    pa.field("status",         pa.string(),  nullable=False),   # 'pending','completed','failed'
    pa.field("partition_date", pa.string(),  nullable=False),   # 'YYYY-MM-DD'
    pa.field("created_at",     pa.string(),  nullable=False),   # ISO timestamp
])

# Audit Log Schema (dùng chung với Thành viên B)
AUDIT_LOG_SCHEMA = {
    "description": "Write Audit Log — mỗi dòng là 1 transaction attempt từ writer",
    "fields": {
        "transaction_id": "str (UUID4) — định danh duy nhất của mỗi lần ghi",
        "writer_id":      "str — định danh writer (e.g. 'writer_0')",
        "operation":      "str — 'APPEND' | 'MERGE' | 'UPDATE' | 'OPTIMIZE'",
        "target_key":     "str — partition_date hoặc id range bị tác động (JSON-serialized)",
        "value":          "dict — payload tóm tắt {num_rows, sum_amount, ids_range}",
        "timestamp":      "float — Unix timestamp lúc commit attempt bắt đầu",
        "committed":      "bool — True nếu commit thành công, False nếu abort",
        "retry_count":    "int — số lần retry trước khi thành công hoặc bỏ cuộc",
        "commit_version": "int | None — Delta Lake version sau khi commit thành công",
        "latency_ms":     "float — tổng thời gian từ lúc bắt đầu đến khi commit/abort",
    },
    "note": "Đây là format chính thức để Thành viên B viết oracle.py"
}


def generate_initial_data(num_rows: int, num_partitions: int) -> pd.DataFrame:
    """Tạo dữ liệu mẫu ban đầu cho bảng transactions."""
    random.seed(42)
    base_date = datetime(2024, 1, 1)
    dates = [(base_date + timedelta(days=i)).strftime("%Y-%m-%d")
             for i in range(num_partitions)]

    rows = []
    for i in range(num_rows):
        rows.append({
            "id":             i,
            "user_id":        random.randint(1, 200),
            "amount":         round(random.uniform(10.0, 1000.0), 2),
            "status":         random.choice(["pending", "completed", "failed"]),
            "partition_date": dates[i % num_partitions],
            "created_at":     datetime.utcnow().isoformat(),
        })
    return pd.DataFrame(rows)


def setup_delta_table() -> None:
    """Task 1.3: Tạo bảng Delta Lake mẫu."""
    print("=" * 60)
    print("🚀 TASK 1: Setup Delta Lake Table")
    print("=" * 60)

    os.makedirs(os.path.dirname(TABLE_PATH), exist_ok=True)

    # Kiểm tra nếu bảng đã tồn tại
    if os.path.exists(os.path.join(TABLE_PATH, "_delta_log")):
        print(f"⚠️  Bảng đã tồn tại tại: {TABLE_PATH}")
        dt = DeltaTable(TABLE_PATH)
        print(f"   Version hiện tại: {dt.version()}")
        print(f"   Số rows: {dt.to_pandas().shape[0]}")
        return

    print(f"📝 Tạo dữ liệu mẫu ({NUM_INITIAL_ROWS} rows, {NUM_PARTITIONS} partitions)...")
    df = generate_initial_data(NUM_INITIAL_ROWS, NUM_PARTITIONS)

    print(f"💾 Ghi vào Delta Lake tại: {TABLE_PATH}")
    write_deltalake(
        TABLE_PATH,
        df,
        schema=TRANSACTIONS_SCHEMA,
        partition_by=["partition_date"],
        mode="overwrite",
    )

    dt = DeltaTable(TABLE_PATH)
    print(f"✅ Bảng tạo thành công!")
    print(f"   - Version: {dt.version()}")
    print(f"   - Rows: {dt.to_pandas().shape[0]}")
    print(f"   - Partitions: {NUM_PARTITIONS}")
    print(f"   - Schema:\n{dt.schema()}")


def save_audit_schema() -> None:
    """Task 1.2/Sync Point: Xuất Audit Log schema để Thành viên B dùng."""
    with open(AUDIT_SCHEMA_PATH, "w", encoding="utf-8") as f:
        json.dump(AUDIT_LOG_SCHEMA, f, indent=2, ensure_ascii=False)
    print(f"\n📋 Audit Log schema đã xuất ra: {AUDIT_SCHEMA_PATH}")
    print("   → Thành viên B dùng file này để viết oracle.py")


def verify_environment() -> None:
    """Task 1.1: Xác nhận môi trường Python sẵn sàng."""
    print("\n🔍 Kiểm tra môi trường:")
    packages = {
        "deltalake": "deltalake",
        "duckdb":    "duckdb",
        "pandas":    "pandas",
        "pyarrow":   "pyarrow",
    }
    all_ok = True
    for display_name, import_name in packages.items():
        try:
            mod = __import__(import_name)
            version = getattr(mod, "__version__", "N/A")
            print(f"   ✅ {display_name} {version}")
        except ImportError:
            print(f"   ❌ {display_name} — CHƯA CÀI")
            all_ok = False

    if not all_ok:
        print("\n⚠️  Chạy: pip install -r requirements.txt")
    else:
        print("\n✅ Môi trường sẵn sàng!")


def query_sample() -> None:
    """Dùng DuckDB để query thử bảng vừa tạo."""
    import duckdb
    print("\n🦆 Query mẫu bằng DuckDB:")
    con = duckdb.connect()
    result = con.execute(f"""
        SELECT partition_date, COUNT(*) as cnt, 
               ROUND(AVG(amount), 2) as avg_amount,
               COUNT(DISTINCT status) as statuses
        FROM delta_scan('{TABLE_PATH}')
        GROUP BY partition_date
        ORDER BY partition_date
    """).fetchdf()
    print(result.to_string(index=False))
    con.close()


if __name__ == "__main__":
    verify_environment()
    setup_delta_table()
    save_audit_schema()
    query_sample()
    print("\n" + "=" * 60)
    print("🎉 Task 1 hoàn thành! Sẵn sàng cho Task 2 (Workload Generator)")
    print("=" * 60)
