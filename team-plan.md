# 📋 Team Plan: Concurrent Writers & Conflict Management
> **Nhóm 2 người | Phân chia theo đúng sở trường: Infrastructure & Correctness**

---

## 🧭 Tổng quan phân công

| | **Thành viên A** — *Infrastructure Engineer* | **Thành viên B** — *Correctness & Analysis Engineer* |
|---|---|---|
| **Trọng tâm** | Xây dựng toàn bộ hạ tầng thực nghiệm, tạo tải và chạy benchmark | Đảm bảo tính đúng đắn dữ liệu, phân tích kết quả và đóng gói báo cáo |
| **Bao gồm Task gốc** | Task 1 + Task 2 + Task 4 (Baseline) | Task 3 + Task 5 + Task 6 |
| **Output chính** | Workload Generator + Conflict Strategy Code | Correctness Oracle + Report + Slides |

---

## 👤 Thành viên A — Infrastructure & Workload Engineer

### Giai đoạn 1 (Tuần 1): Setup môi trường & Cài đặt
> Mục tiêu: Có một môi trường Lakehouse chạy được, có bảng dữ liệu mẫu, có Docker/MinIO nếu cần.

**Việc cụ thể:**
- [ ] **1.1** Cài đặt môi trường Python: `delta-spark` / `deltalake` (Python-native) + `duckdb` + `pandas`
- [ ] **1.2** Chọn storage backend: Local filesystem (đơn giản) hoặc MinIO (giả lập S3, thực tế hơn)
- [ ] **1.3** Tạo bảng Delta Lake mẫu với schema rõ ràng:
  ```
  Table: transactions
  - id (PK), user_id, amount, status, partition_date
  ```
- [ ] **1.4** Viết `setup.py` / `Makefile` để ai cũng cài được môi trường trong 1 lệnh
- [ ] **1.5** Phát biểu giả thuyết cùng Thành viên B, ghi vào `HYPOTHESIS.md`

---

### Giai đoạn 2 (Tuần 2): Xây dựng Workload Generator
> Mục tiêu: File `workload_generator.py` chạy được, giả lập đủ 3 access patterns bắt buộc.

**Việc cụ thể:**
- [ ] **2.1** Xây dựng lớp `Writer` cơ sở: nhận payload, ghi vào Delta table, trả về kết quả commit (success/fail/retry_count)
- [ ] **2.2** Cài đặt **Pattern A — Disjoint Writes**: N workers, mỗi worker chỉ ghi vào 1 partition riêng biệt (ví dụ: `partition_date = '2024-01-01'` đến `'2024-01-08'`)
- [ ] **2.3** Cài đặt **Pattern B — Overlapping Writes (True Conflicts)**: N workers cùng MERGE/UPDATE trên cùng 1 partition hoặc cùng tập `id`
- [ ] **2.4** Cài đặt **Pattern C — Maintenance Concurrent**: Một goroutine/thread chạy `OPTIMIZE` / file compaction, trong khi các writer khác đang Append liên tục
- [ ] **2.5** Thêm tham số điều chỉnh: `num_writers`, `num_rows_per_writer`, `conflict_ratio`, `pattern_type`
- [ ] **2.6** Bàn giao cho Thành viên B: danh sách chính xác (writer_id, rows_committed, timestamp) mà generator đã ghi thành công → làm đầu vào cho Oracle

---

### Giai đoạn 3 (Tuần 3): Cài đặt Conflict Resolution Strategy
> Mục tiêu: File `conflict_strategy.py` so sánh được Baseline vs Proposed Method.

**Việc cụ thể:**
- [ ] **3.1** Chạy **Baseline**: Cấu hình mặc định của Delta Lake, không can thiệp, ghi nhận Throughput + Abort rate
- [ ] **3.2** Cài đặt **Strategy 1 — Exponential Backoff + Jitter**:
  ```python
  wait = min(cap, base * 2^attempt) + random.uniform(0, jitter)
  ```
- [ ] **3.3** Cài đặt **Strategy 2 — Selective Serialization**: Bộ điều phối nhỏ kiểm tra xem 2 writer có overlap key không; nếu có thì queue, nếu không thì thả song song
- [ ] **3.4** Gắn **Idempotent Commit ID**: Mỗi transaction có UUID, tránh duplicate khi retry sau timeout
- [ ] **3.5** Đảm bảo log đầy đủ metrics ra file: `throughput`, `conflict_rate`, `retry_count`, `p95_latency`, `abort_count`

---

## 👤 Thành viên B — Correctness & Analysis Engineer

### Giai đoạn 1 (Tuần 1): Thiết kế Correctness Oracle
> Mục tiêu: File `oracle.py` có thể nhận danh sách commit logs từ Thành viên A và tính ra "Ground Truth" độc lập.

**Việc cụ thể:**
- [ ] **1.1** Đọc kỹ spec của từng access pattern (Disjoint / Overlapping / Maintenance) từ Thành viên A để hiểu cấu trúc dữ liệu đầu vào
- [ ] **1.2** Thiết kế **Write Audit Log schema**: Mỗi dòng ghi gồm: `(transaction_id, writer_id, operation, target_key, value, timestamp, committed: bool)`
- [ ] **1.3** Viết hàm `compute_ground_truth(audit_log)` → trả về DataFrame trạng thái dữ liệu đúng tuyệt đối, áp dụng đúng thứ tự: committed transactions theo timestamp tăng dần, deduplicate bằng `transaction_id`
- [ ] **1.4** Viết hàm `validate(table_state, ground_truth)` → trả về dict:
  ```python
  {
    "LostUpdateCount": 0,      # PHẢI = 0
    "WrongFinalRows": 0,       # PHẢI = 0
    "DuplicateRows": 0,        # PHẢI = 0
    "passed": True
  }
  ```
- [ ] **1.5** Viết unit test cho Oracle với dữ liệu giả: test trường hợp mất dữ liệu, trùng lặp, sai giá trị

---

### Giai đoạn 2 (Tuần 2): Tích hợp Oracle vào Pipeline thực nghiệm
> Mục tiêu: Pipeline đầy đủ: Workload Generator → Table → Oracle Validation tự động sau mỗi run.

**Việc cụ thể:**
- [ ] **2.1** Tích hợp `oracle.py` vào pipeline: sau khi Thành viên A chạy xong 1 batch commit, tự động gọi `validate()`
- [ ] **2.2** Wrap thành hàm `run_experiment(pattern, strategy, num_writers, num_runs)` trả về kết quả có đầy đủ: throughput + correctness flag
- [ ] **2.3** Thêm cơ chế **Multiple Runs**: Loop 5–10 lần với cùng config, thu thập phân phối của Throughput và Latency (không chỉ trung bình mà có p50, p95, stddev)
- [ ] **2.4** Đảm bảo nếu `validate()` fail (LostUpdateCount > 0) thì Throughput bị force về 0 (theo quy tắc của đề tài)
- [ ] **2.5** Export kết quả ra file `results/experiment_results.csv` với đầy đủ cột để vẽ biểu đồ

---

### Giai đoạn 3 (Tuần 3): Phân tích, vẽ biểu đồ & Đóng gói báo cáo
> Mục tiêu: Bộ slide 4 trang + README 1 trang + biểu đồ so sánh rõ ràng.

**Việc cụ thể:**
- [ ] **3.1** Phân tích kết quả CSV, vẽ ít nhất 2 biểu đồ:
  - Biểu đồ 1: **Throughput** (Baseline vs Strategy 1 vs Strategy 2) theo `num_writers`
  - Biểu đồ 2: **p95 Commit Latency** boxplot theo pattern (Disjoint / Overlapping / Maintenance)
- [ ] **3.2** **Bắt buộc**: Tìm và viết phân tích **ít nhất 1 ca thất bại** (Failure Case):
  - Kịch bản nào mà giải pháp của nhóm không hiệu quả hơn Baseline? Tại sao?
  - Viết thành 1 đoạn văn rõ ràng trong README
- [ ] **3.3** Viết **README.md 1 trang** theo cấu trúc:
  ```
  Problem → Hypothesis → Setup → Baseline Result → 
  Proposed Method → Result → Failure Case → Conclusion
  ```
- [ ] **3.4** Làm **4 slide Pitch**:
  - Slide 1: Production Pain Point + Hypothesis phát biểu bằng 1 câu
  - Slide 2: Experiment Setup (Architecture diagram + 3 patterns)
  - Slide 3: Kết quả (Biểu đồ Throughput + Latency, so sánh Baseline vs Proposed)
  - Slide 4: Production Decision — Có deploy không? Rủi ro còn lại? Next steps?
- [ ] **3.5** Tổng hợp toàn bộ `requirements.txt`, kiểm tra lại code Thành viên A chạy được end-to-end

---

## 🔗 Điểm giao nhau (Sync Points) giữa 2 người

| Thời điểm | Nội dung cần sync |
|---|---|
| **Cuối Tuần 1** | A bàn giao schema bảng + Audit Log format → B bắt đầu viết Oracle |
| **Đầu Tuần 2** | B bàn giao `oracle.py` draft → A tích hợp vào generator để log đúng format |
| **Cuối Tuần 2** | Cả 2 cùng chạy thử Pipeline end-to-end, kiểm tra Oracle đã validate đúng |
| **Cuối Tuần 3** | A hoàn thiện metrics log → B vẽ biểu đồ và viết báo cáo |

---

## 📁 Cấu trúc thư mục đề xuất

```
concurrent-writers-research/
│
├── setup.py / requirements.txt        ← A setup
├── HYPOTHESIS.md                      ← Cả 2 viết chung
│
├── src/
│   ├── workload_generator.py          ← A
│   ├── conflict_strategy.py           ← A
│   ├── oracle.py                      ← B
│   └── experiment_runner.py           ← B (tích hợp tất cả)
│
├── results/
│   └── experiment_results.csv         ← Output tự động
│
├── notebooks/
│   └── analysis.ipynb                 ← B vẽ biểu đồ
│
├── report/
│   ├── README.md                      ← B (1 trang)
│   └── slides.pdf                     ← B (4 slides)
│
└── tests/
    └── test_oracle.py                 ← B unit tests
```

---

> [!TIP]
> **Gợi ý về stack**: Dùng `deltalake` (Python-native, không cần Spark/JVM) + `duckdb` để query kết quả. Nhanh hơn rất nhiều so với PySpark cho môi trường research nhỏ, dễ debug hơn.

> [!WARNING]
> **Lưu ý quan trọng**: Cả 2 người phải đồng thuận với nhau về **Audit Log format** (schema) trước khi ai bắt đầu code. Đây là điểm khớp nối duy nhất giữa 2 phần, nếu sai format thì Oracle sẽ không validate được đúng.
