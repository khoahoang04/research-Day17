# 📐 HYPOTHESIS.md
> Viết chung bởi Thành viên A & Thành viên B

---

## 🎯 Vấn đề (Problem Statement)

Trong môi trường Lakehouse hiện đại (Delta Lake), nhiều writer có thể đồng thời ghi vào cùng một bảng. Cơ chế Optimistic Concurrency Control (OCC) mặc định của Delta Lake xử lý conflict bằng cách abort và retry toàn bộ transaction nếu phát hiện xung đột. Điều này dẫn đến:

1. **Throughput giảm** khi số lượng writer tăng (conflict càng nhiều)
2. **Latency tăng** do retry storm (nhiều writer cùng retry một lúc)
3. **Abort rate cao** trong Pattern B (Overlapping Writes)

---

## 💡 Giả thuyết (Hypothesis)

> **H1**: Với access pattern **Disjoint Writes** (mỗi writer ghi vào partition riêng biệt), Delta Lake OCC mặc định đạt throughput gần-tuyến-tính theo số writer, vì hầu như không có conflict thực sự.

> **H2**: Với access pattern **Overlapping Writes** (nhiều writer cùng MERGE/UPDATE trên cùng partition), áp dụng **Exponential Backoff + Jitter** sẽ giảm abort rate ≥ 30% so với retry ngay lập tức (Baseline), vì tránh được "thundering herd".

> **H3**: **Selective Serialization** (chỉ serialize các writer có overlap key thực sự, thả song song những writer không overlap) sẽ đạt throughput cao hơn Baseline trong Pattern B, đặc biệt khi `conflict_ratio` thấp (< 30%).

> **H4**: Trong Pattern C (Maintenance Concurrent), chạy `OPTIMIZE` song song với Append writers sẽ không ảnh hưởng đáng kể đến throughput của writers (< 5% degradation) nhờ Delta Lake's transaction isolation.

---

## 📊 Định nghĩa thành công (Success Criteria)

| Metric | Baseline | Target (Proposed) |
|--------|----------|-------------------|
| Throughput (rows/s) | X | ≥ 1.2X |
| Abort rate (%) | Y | ≤ 0.7Y |
| p95 Commit Latency (ms) | Z | ≤ 0.9Z |
| LostUpdateCount | 0 | **PHẢI = 0** (correctness) |
| DuplicateRows | 0 | **PHẢI = 0** (correctness) |

---

## ⚠️ Failure Case dự đoán

- Khi `conflict_ratio` = 100% (tất cả writer cùng ghi 1 key), **Selective Serialization** bị degenerate thành pure serialization → throughput = 1/N so với N writers song song.
- Khi `num_writers` rất lớn (> 20), overhead của coordinator trong Selective Serialization có thể làm latency tăng thay vì giảm.

---

*Cập nhật lần cuối: 2026-10-04 bởi Thành viên A*
