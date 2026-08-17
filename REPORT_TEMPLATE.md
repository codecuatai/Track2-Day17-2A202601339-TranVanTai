# Báo cáo LAB 17 — Data Pipeline Engineering

**Họ tên:** Trần Văn Tài  **Lớp:** E403  **Ngày:** 17/08/2026

---

## 0 · Kết quả `make verify`

<details>
<summary>Output make verify sau khi hoàn thành</summary>

```
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
LAB 17 · make verify
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
run 1/3 … 37.5s
run 2/3 … 32.6s
run 3/3 … 31.8s

BẢNG                  ỔN ĐỊNH          SỐ HÀNG     KỲ VỌNG   GHI CHÚ
──────────────────────────────────────────────────────────────────────────
gold_training_set     ✓ ok              12,480      12,480   ✓
gold_feature_daily    ✓ ok               9,100       9,100   ✓
gold_doc_chunks       ✓ ok              31,200      31,200   ✓
quarantine_tickets    ✓ ok                 312         312   ✓

CHECKSUM từng lượt
──────────────────────────────────────────────────────────────────────────
gold_training_set     8dd7c98653    8dd7c98653    8dd7c98653   ✓
gold_feature_daily    3db448685c    3db448685c    3db448685c   ✓
gold_doc_chunks       92d8e50131    92d8e50131    92d8e50131   ✓
quarantine_tickets    ebb89036fb    ebb89036fb    ebb89036fb   ✓

KIỂM TRA KHÁC
──────────────────────────────────────────────────────────────────────────
dbt test                                    ✓ 11/11 pass
silver_tickets.priority ∈ 1..4, không NULL  ✓ sạch
quarantine_tickets đúng số bản ghi lỗi      ✓ 312 / 312
gold_training_set: 1 hàng / 1 ticket        ✓ không lặp
dashboard rows scanned                      ✗ 5,000,000 → 5,000,000
  số file parquet                           ✗ 5,000 → 5,000
  kết quả truy vấn không đổi                ✓
DAG: catchup / max_active_runs              ✓ False / 1

TỔNG KẾT
──────────────────────────────────────────────────────────────────────────
✓  1 · gold_training_set idempotent & đúng số hàng
✓  2 · gold_feature_daily đủ hàng (dữ liệu về muộn)
✓  3 · contract + quarantine + dbt test
✓  4 · gold_doc_chunks vẫn ổn định (đối chứng)
──────────────────────────────────────────────────────────────────────────
4/4 tiêu chí đạt
```

</details>

Tổng kết: **4 / 4 tiêu chí đạt**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lần retry/backfill, `gold_training_set` tăng thêm; sau ba lượt có 38.750 hàng và 12.480 ticket bị lặp. |
| **Nguyên nhân** | Model incremental không khai báo `unique_key` và strategy nên dbt sinh thao tác chèn thêm. Khi cùng partition bị chạy lại hoặc một ticket CDC được cập nhật ở ngày sau, cùng entity tiếp tục được insert thay vì cập nhật, làm pipeline không idempotent. `catchup=True` và không giới hạn active run làm tăng khả năng kích hoạt lỗi nhưng không phải root cause. |
| **Cách khắc phục** | Trong `gold_training_set.sql`, dùng `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong DAG, đặt `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng, checksum thay đổi · Sau: 12.480 hàng, không lặp · checksum ba lượt: `8dd7c98653` giống nhau. |

---

## 2 · Bảng đặc trưng theo ngày thiếu hàng ở các ngày quá khứ

| | |
|---|---|
| **Triệu chứng** | `gold_feature_daily` ổn định nhưng chỉ có 8.645/9.100 cặp ngày–customer; 455 cặp của các ngày cũ bị thiếu. |
| **P99 độ trễ đo được** | **2,7258 ngày** |
| **Lookback đã chọn** | **3 ngày** — làm tròn lên từ P99; đồng thời bao phủ max quan sát được là 2,9447 ngày. |
| **Nguyên nhân** | Watermark `event_date > max(event_date)` giả định dữ liệu đến đúng thứ tự event-time. Event xảy ra ngày cũ nhưng được ingest vài ngày sau luôn nhỏ hơn watermark và bị bỏ qua vĩnh viễn. |
| **Cách khắc phục** | Tính lại cửa sổ ba ngày; dùng khóa ghép `['event_date', 'customer_id']` và `delete+insert` để hàng được tính lại thay thế kết quả cũ thay vì append. |
| **Bằng chứng** | Trước: 8.645 hàng · Sau: 9.100 hàng · checksum ba lượt: `3db448685c` giống nhau. |

Vì sao chọn P99 làm căn cứ thay vì `max`? Chi phí của mỗi lựa chọn là gì?

> P99 là ngưỡng ổn định hơn trước một vài ngoại lệ cực đoan và cho phép cân bằng
> giữa độ phủ với chi phí. Max có thể tăng mạnh chỉ vì một outlier, khiến mọi
> lượt chạy sau phải quét lại một cửa sổ quá rộng. Trong bộ dữ liệu này P99 là
> 2,7258 ngày và max là 2,9447 ngày, nên cửa sổ ba ngày vừa dựa trên P99 vừa bao
> phủ toàn bộ độ trễ quan sát được. Mỗi ngày lookback bổ sung làm tăng số event
> phải đọc và số aggregate phải tính lại ở mọi lượt chạy sau đó.

---

## 3 · Kiểu dữ liệu cột priority thay đổi giữa chu kỳ

| | |
|---|---|
| **Triệu chứng** | Pipeline vẫn chạy và 9/9 test vẫn pass, nhưng Silver có 6.606 hàng priority NULL/ngoài miền; quarantine rỗng. |
| **Nguyên nhân** | `try_cast` biến các nhãn hợp lệ sau schema evolution thành NULL nhưng lại chấp nhận `0`, `5`, `-1` vì chúng vẫn là số. Silver không lọc bản ghi không chuẩn hóa được, quarantine dùng `where false`, còn contract và domain test đều bị tắt nên lỗi diễn ra âm thầm. |
| **Ba nhóm giá trị `priority` và cách xử lý từng nhóm** | `1..4` giữ nguyên; `urgent/high/medium/low` map thành `1/2/3/4`; `P1`, `P2`, `unknown`, `0`, `5`, `-1`, rỗng và NULL đưa vào quarantine. |
| **Cách khắc phục** | Dùng chung macro `CASE` cho Silver và quarantine; lọc bản ghi lỗi trước khi xếp hạng CDC; bật contract; thêm `not_null` và `accepted_values [1,2,3,4]`. |
| **Bằng chứng** | `quarantine_tickets` = 312 hàng · `silver_tickets` = 12.480 ticket · priority sạch · `dbt test` 11/11 pass. |

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Bronze nên giữ nguyên payload để có bằng chứng điều tra và khả năng replay.
> Silver là nơi áp dụng contract, chuẩn hóa các biểu diễn tương đương và định
> tuyến dữ liệu thật sự sai vào quarantine. Không nên dừng toàn DAG vì 312 bản
> ghi lỗi trong 14.300 CDC row sẽ chặn cả dữ liệu hợp lệ và các nhánh event/RAG;
> quarantine cho phép pipeline phục vụ downstream đồng thời tạo hàng đợi để vận
> hành xử lý dữ liệu lỗi.

---

## 4 · *(mở rộng, không bắt buộc)* Bài trong EXTRA.md

| | |
|---|---|
| **Bài đã làm** | Không làm; ba nhiệm vụ chính đã đạt 100 điểm. |
| **Nguyên nhân** | Dashboard chậm do 5.000 file nhỏ và layout không hỗ trợ partition pruning; đây là bài thưởng, không thuộc 4 tiêu chí chính. |
| **Cách khắc phục** | Chưa triển khai compact/partition. |
| **Bằng chứng** | Core verify đạt 4/4; dashboard giữ nguyên hash nhưng rows scanned vẫn 5.000.000. |

---

## 5 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Grain, natural key và câu lệnh ghi thật sự mà incremental model sinh ra khi retry. |
| 2 | Quan hệ giữa event-time và ingestion-time, phân bố độ trễ và watermark hiện tại. |
| 3 | Phân bố giá trị raw, contract đang thực sự được bật hay chưa và đường đi của bản ghi bị loại. |
