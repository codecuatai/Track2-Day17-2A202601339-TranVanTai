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
run 1/3 … 31.9s
run 2/3 … 38.8s
run 3/3 … 33.1s

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
dashboard rows scanned                      ✓ 5,000,000 → 9,324 (536.3×)
  số file parquet                           ✓ 5,000 → 14
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

Tổng kết: **4 / 4 tiêu chí chính đạt** · **Bài mở rộng A đạt** ·
**Bài mở rộng B đạt** · Tự chấm theo rubric: **110 / 100**

---

## 1 · Kích thước bảng training tăng sau mỗi lần chạy

| | |
|---|---|
| **Triệu chứng** | Sau mỗi lần retry/backfill, `gold_training_set` tăng thêm; sau ba lượt có 38.750 hàng và 12.480 ticket bị lặp. |
| **Nguyên nhân** | Model incremental không khai báo `unique_key` và strategy nên dbt sinh thao tác chèn thêm. Khi cùng partition bị chạy lại hoặc một ticket CDC được cập nhật ở ngày sau, cùng entity tiếp tục được insert thay vì cập nhật, làm pipeline không idempotent. `catchup=True` và không giới hạn active run làm tăng khả năng kích hoạt lỗi nhưng không phải root cause. |
| **Cách khắc phục** | Trong `gold_training_set.sql`, dùng `unique_key='ticket_id'` và `incremental_strategy='merge'`. Trong DAG, đặt `catchup=False`, `max_active_runs=1`. |
| **Bằng chứng** | Trước: 38.750 hàng, checksum thay đổi · Sau: 12.480 hàng, không lặp · checksum ba lượt: `8dd7c98653` giống nhau. |

### Evidence kiểm chứng — Nhiệm vụ 1

Evidence baseline được lưu từ lần `make verify` trước khi sửa; evidence sau sửa
được lấy từ ba lượt chạy liên tiếp của `make verify` và truy vấn trực tiếp
warehouse ở chế độ read-only.

| Kiểm tra | Trước khi sửa | Sau khi sửa | Kết luận |
|---|---:|---:|---|
| Tổng số hàng | 38.750 | 12.480 | Khớp expected 12.480 |
| Số `ticket_id` phân biệt | 12.480 | 12.480 | Grain đúng 1 ticket/entity |
| Số hàng thừa theo grain | 26.270 | 0 | Không còn append trùng |
| Ticket xuất hiện nhiều lần | 12.480 | 0 | Retry không tạo duplicate |
| Checksum lượt 1 | `7c461563f4` | `8dd7c98653` | Sau sửa ổn định |
| Checksum lượt 2 | `d11657ff21` | `8dd7c98653` | Sau sửa ổn định |
| Checksum lượt 3 | `2b76a4f850` | `8dd7c98653` | Sau sửa ổn định |
| DAG `catchup / max_active_runs` | `True / None` | `False / 1` | Không backfill/chạy chồng ngoài ý muốn |

Truy vấn invariant sau lần chạy cuối trả về:

```text
gold_training_set
rows = 12.480
count(distinct ticket_id) = 12.480
rows - count(distinct ticket_id) = 0
```

Dấu vết implementation: `dbt/models/gold/gold_training_set.sql:29-30` khai báo
`unique_key='ticket_id'` và `incremental_strategy='merge'`;
`dags/ai_training_pipeline.py:36-37` khai báo `catchup=False` và
`max_active_runs=1`. Kết quả ba checksum bằng nhau chứng minh tính idempotent,
không chỉ chứng minh đúng số hàng ở một thời điểm.

#### Vì sao cách sửa và evidence trên là hợp lý?

1. **Vì sao khóa là `ticket_id`?** Grain được yêu cầu của training set là một
   hàng cho một ticket. Vì vậy natural key phải là `ticket_id`; dùng ngày chạy
   hoặc thời điểm CDC làm khóa sẽ biến các phiên bản của cùng ticket thành các
   hàng khác nhau và tiếp tục tạo duplicate.

2. **Vì sao dùng `merge`?** Ticket là entity có thể được cập nhật qua nhiều bản
   ghi CDC. `merge` tìm hàng đích theo `ticket_id`: ticket mới được insert, ticket
   đã tồn tại được update. Do đó cùng input chạy lại vẫn hội tụ về một trạng thái.
   Append thuần không có bước đối chiếu khóa nên mỗi lần retry lại cộng thêm hàng.

3. **Vì sao không chỉ sửa DAG?** `catchup=False` và `max_active_runs=1` giảm số
   lần backfill và ngăn hai DAG run ghi đồng thời, nhưng retry vẫn có thể xảy ra
   sau timeout hoặc worker restart. Tính idempotent phải được bảo đảm tại câu
   lệnh ghi dữ liệu; cấu hình DAG chỉ là lớp phòng vệ bổ sung.

4. **Vì sao phải kiểm tra cả count, uniqueness và checksum?** Đúng 12.480 hàng
   chưa đủ vì dữ liệu có thể vừa thiếu vừa trùng với tổng số tình cờ không đổi.
   `count(distinct ticket_id) = count(*)` chứng minh đúng grain; ba checksum bằng
   nhau chứng minh nội dung không tiếp tục biến đổi qua các lần retry. Baseline có
   ba checksum khác nhau chính là bằng chứng pipeline cũ không idempotent.

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

### Evidence kiểm chứng — Nhiệm vụ 2

Phân bố độ trễ được đo trực tiếp từ
`date_diff('second', event_time, _ingested_at) / 86400.0` trên toàn bộ
`bronze_events`:

| Chỉ số độ trễ | Kết quả |
|---|---:|
| P50 | 0,1281 ngày |
| P95 | 1,8137 ngày |
| P99 | 2,7258 ngày |
| Max | 2,9447 ngày |
| Bản ghi đến muộn hơn 1 ngày | 5,0509% |

| Kiểm tra coverage | Trước khi sửa | Sau khi sửa | Kỳ vọng |
|---|---:|---:|---:|
| Tổng cặp ngày–customer | 8.645 | 9.100 | 9.100 |
| Số cặp thiếu | 455 | 0 | 0 |
| Số ngày | — | 14 | 14 |
| Số customer | — | 650 | 650 |
| Phép đối chiếu đầy đủ | — | `14 × 650 = 9.100` | 9.100 |
| Checksum ba lượt | `4eee63cd82` ổn định nhưng thiếu | `3db448685c` × 3 | Đúng và ổn định |

Phép `EXCEPT` giữa tập cặp `(event_date, customer_id)` từ `silver_events` và
`gold_feature_daily` trả về **0 hàng**. Điều này loại trừ trường hợp chỉ đạt đủ
số lượng nhưng vẫn sai coverage. Dấu vết implementation:
`dbt/models/gold/gold_feature_daily.sql:34-35` dùng khóa ghép cùng
`delete+insert`; dòng 55 dùng điều kiện `>= max(event_date) - interval 3 day`.

#### Vì sao cách sửa và evidence trên là hợp lý?

1. **Vì sao watermark cũ sai dù checksum ổn định?** Điều kiện chỉ lấy ngày lớn
   hơn ngày lớn nhất ở bảng đích giả định ingest-time và event-time cùng thứ tự.
   Một event của ngày cũ đến sau watermark sẽ bị loại ở mọi lượt tiếp theo. Vì
   vậy checksum `4eee63cd82` lặp lại chỉ chứng minh kết quả thiếu được lặp lại ổn
   định, không chứng minh kết quả đúng.

2. **Vì sao chọn ba ngày?** P99 là 2,7258 ngày và độ trễ lớn nhất quan sát được
   là 2,9447 ngày. Làm tròn lên ba ngày bao phủ cả ngưỡng vận hành P99 lẫn toàn bộ
   seed hiện tại. Chọn hai ngày chắc chắn bỏ sót phần đuôi; chọn cửa sổ dài hơn
   vẫn đúng nhưng phải đọc và aggregate lại nhiều partition hơn ở mọi lần chạy.

3. **Vì sao dùng khóa ghép và `delete+insert`?** Grain của model là một hàng cho
   một cặp `(event_date, customer_id)`. Khi mở lookback, các cặp thuộc cửa sổ sẽ
   được tính lại. `delete+insert` xóa đúng các khóa ghép được xử lý rồi ghi phiên
   bản aggregate mới, tránh cộng dồn kết quả cũ với kết quả tính lại.

4. **Vì sao `9.100 = 14 × 650` vẫn cần phép `EXCEPT`?** Tổng số hàng đúng vẫn có
   thể che giấu một cặp bị thiếu và một cặp khác bị trùng. Phép `EXCEPT = 0` chứng
   minh mọi cặp có ở nguồn đều có ở Gold; checksum giữ nguyên qua ba lượt chứng
   minh việc tính lại cửa sổ không làm model mất ổn định.

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

### Evidence kiểm chứng — Nhiệm vụ 3

Phân bố đầy đủ của 14.300 bản ghi CDC raw cho thấy ba nhóm dữ liệu tách biệt:

| Nhóm raw | Giá trị và số hàng | Tổng |
|---|---|---:|
| Số hợp lệ | `1`: 1.705 · `2`: 1.683 · `3`: 1.710 · `4`: 1.748 | 6.846 |
| Nhãn hợp lệ cần map | `urgent`: 1.819 · `high`: 1.695 · `medium`: 1.783 · `low`: 1.845 | 7.142 |
| Không hợp lệ | `0`: 49 · rỗng: 43 · `P1`: 39 · `unknown`: 39 · `P2`: 38 · `5`: 37 · NULL: 35 · `-1`: 32 | 312 |
| **Tổng** |  | **14.300** |

Kết quả định tuyến 312 bản ghi lỗi vào quarantine khớp tuyệt đối với phân bố
raw; không có bản ghi lỗi bị mất hoặc lọt vào Silver:

| `reject_reason` trong `quarantine_tickets` | Số hàng |
|---|---:|
| `priority dạng số nằm ngoài miền 1..4` | 118 |
| `priority không nhận diện được` | 116 |
| `priority rỗng hoặc NULL` | 78 |
| **Tổng** | **312** |

Phân bố sau chuẩn hóa và CDC dedup trong `silver_tickets`:

| Priority chuẩn hóa | Số hàng |
|---:|---:|
| 1 | 3.134 |
| 2 | 3.029 |
| 3 | 3.115 |
| 4 | 3.202 |
| **Tổng** | **12.480** |

Truy vấn invariant cuối cùng trả về `rows = 12.480`,
`count(distinct ticket_id) = 12.480`, `bad_priority = 0`. So với baseline,
Silver giảm từ **6.606 hàng sai xuống 0**, quarantine tăng từ **0 lên đúng 312**,
và dbt test tăng từ **9/9 lên 11/11 pass** sau khi bổ sung `not_null` và
`accepted_values`.

Dấu vết implementation: macro dùng chung bắt đầu tại
`dbt/macros/normalize_priority.sql:47`; Silver gọi macro tại
`dbt/models/silver/silver_tickets.sql:42`; quarantine lấy đúng phần macro trả
NULL tại `dbt/models/silver/quarantine_tickets.sql:36`; contract được bật tại
`dbt/models/silver/schema.yml:22-23`, còn hai domain test nằm tại dòng 40-44.

#### Vì sao cách sửa và evidence trên là hợp lý?

1. **Vì sao không dùng riêng `try_cast`?** `try_cast('urgent' as integer)` trả
   NULL dù `urgent` là biểu diễn nghiệp vụ hợp lệ của priority 1; ngược lại,
   `try_cast('5' as integer)` thành công dù 5 nằm ngoài contract 1..4. Chuẩn hóa
   vì vậy phải kiểm tra cả biểu diễn lẫn miền giá trị, không chỉ kiểu cú pháp.

2. **Vì sao dùng chung một macro?** Nếu Silver và quarantine viết hai biểu thức
   `CASE` riêng, chỉ cần một bên được cập nhật khác bên còn lại là bản ghi có thể
   vừa bị loại khỏi Silver vừa không xuất hiện trong quarantine, hoặc xuất hiện ở
   cả hai. Cùng một macro tạo hai nhánh bù nhau: kết quả khác NULL đi vào Silver,
   kết quả NULL đi vào quarantine.

3. **Vì sao 13.988 raw hợp lệ chỉ còn 12.480 Silver?** Bronze chứa lịch sử CDC,
   nên một ticket có thể có nhiều phiên bản hợp lệ. Silver xếp hạng theo ticket
   và giữ phiên bản hợp lệ mới nhất; vì vậy 13.988 là số bản ghi CDC hợp lệ còn
   12.480 là số entity ticket phân biệt. Đây là giảm do dedup đúng grain, không
   phải mất dữ liệu.

4. **Vì sao con số 312 là bằng chứng mạnh?** Ba nhóm reject cộng lại đúng
   `118 + 116 + 78 = 312`, bằng đúng tổng các raw value không hợp lệ. Đồng thời
   truy vấn Silver trả `bad_priority = 0`. Hai phía cùng khớp chứng minh bản ghi
   lỗi đã được định tuyến đầy đủ thay vì chỉ bị lọc bỏ âm thầm.

5. **Vì sao cần cả contract và dbt test?** Contract bảo vệ schema/kiểu dữ liệu
   khi model được materialize, nhưng một số nguyên vẫn có thể sai miền. Test
   `not_null` và `accepted_values [1,2,3,4]` bảo vệ semantics sau khi build. Hai
   lớp kiểm tra phát hiện hai loại lỗi khác nhau và biến lỗi âm thầm ban đầu thành
   điều kiện có thể quan sát trong CI hoặc vận hành.

### Evidence hồi quy — bảng đối chứng basic

`gold_doc_chunks` không thuộc ba vị trí cần sửa nhưng được dùng để phát hiện tác
động ngoài ý muốn. Cả ba lượt vẫn có **31.200 hàng** và cùng checksum
`92d8e50131`; hai test `unique`/`not_null` của `chunk_id` vẫn pass. Vì vậy thay
đổi ở ba nhiệm vụ basic không làm hỏng nhánh transcript/RAG.

Câu hỏi thiết kế: nên chặn ở tầng Bronze hay Silver? Vì sao **không** để
pipeline dừng khi gặp bản ghi lỗi?

> Bronze nên giữ nguyên payload để có bằng chứng điều tra và khả năng replay.
> Silver là nơi áp dụng contract, chuẩn hóa các biểu diễn tương đương và định
> tuyến dữ liệu thật sự sai vào quarantine. Không nên dừng toàn DAG vì 312 bản
> ghi lỗi trong 14.300 CDC row sẽ chặn cả dữ liệu hợp lệ và các nhánh event/RAG;
> quarantine cho phép pipeline phục vụ downstream đồng thời tạo hàng đợi để vận
> hành xử lý dữ liệu lỗi.

---

## 4 · Bài mở rộng A — Tối ưu dashboard Parquet

| | |
|---|---|
| **Triệu chứng** | Dataset chỉ có 130.683 hàng nhưng bị chia thành 5.000 file nhỏ; truy vấn một customer trong một ngày vẫn báo 5.000.000 rows scanned và mất khoảng 2.422,4 ms trên máy đo. |
| **Nguyên nhân** | File không partition nên engine phải mở toàn bộ 5.000 file trước khi biết file nào hữu ích. Predicate `strftime(event_time, ...)` bọc cột trong hàm nên không sargable và không thể dùng thông tin partition/min-max để pruning. Chi phí đọc còn bị làm tròn theo từng file, tạo small-file amplification. |
| **Cách khắc phục** | `tools/compact.py` ghi lại dataset partition theo `event_date` (14 giá trị thay vì 650 customer), sắp theo `event_date, customer_name, event_time, event_id`, dùng row group 2.048. Query mới đọc recursive với `hive_partitioning=true` và lọc trực tiếp `event_date = DATE '2026-08-09'`. |
| **Bằng chứng** | Rows scanned: **5.000.000 → 9.324**, giảm **536,3×** (yêu cầu ≥10×) · files: **5.000 → 14** · rows on disk: **130.683 → 130.683** · result hash: `4379e4c5d9f3` không đổi · thời gian tham khảo: **2.422,4 → 25,6 ms**. |

Partition theo ngày tạo 14 thư mục có kích thước hợp lý và cho phép loại 13
ngày trước khi mở file. Không partition theo customer vì 650 giá trị sẽ tái tạo
small-file problem. Sắp theo customer giữ các hàng của ACME liền nhau để min/max
của row group có ích cho các dashboard filter khác; row group 2.048 tránh gói
cả ngày vào một row group có min/max quá rộng.

## 5 · Bài mở rộng B — Consumer bị kill giữa batch

| | |
|---|---|
| **Triệu chứng** | Implementation ban đầu commit offset trước khi ghi. Nếu chết ở batch 7 (500 message/batch), offset đã tiến tới 3.500 trong khi database mới có 3.000 hàng; restart bỏ qua batch 7 và kết quả chỉ còn 19.500/20.000 hàng. |
| **Nguyên nhân** | Commit-before-write là at-most-once: transport coi batch đã xử lý dù side effect chưa tồn tại. Chỉ đảo thành write-before-commit tạo at-least-once nhưng batch có thể replay; với INSERT thuần, replay sẽ gây trùng. Exactly-once không được cung cấp xuyên qua ranh giới offset file và DuckDB transaction. |
| **Cách khắc phục** | Ghi và commit transaction DuckDB trước, đặt điểm crash sau write và chỉ commit offset cuối cùng. Thêm primary key `event_id`; mỗi batch dùng vectorized `from_json` và `ON CONFLICT (event_id) DO UPDATE` để replay idempotent. `DO UPDATE` được chọn thay vì `DO NOTHING` để payload mới/corrected có thể thay thế dữ liệu cũ khi replay. |
| **Bằng chứng** | Không sự cố: 20.000/20.000 · crash batch 7: exit 137, committed offset **3.000** · restart xử lý 17.000 message (bao gồm replay batch 7) · cuối cùng **20.000 hàng / 20.000 event_id**, không mất, không trùng, `C == A`, crash-test **ĐẠT**. |

---

## 6 · Tổng kết

| Nhiệm vụ | Khi tiếp nhận một hệ thống chưa quen, tôi sẽ kiểm tra điều này trước tiên |
|---|---|
| 1 | Grain, natural key và câu lệnh ghi thật sự mà incremental model sinh ra khi retry. |
| 2 | Quan hệ giữa event-time và ingestion-time, phân bố độ trễ và watermark hiện tại. |
| 3 | Phân bố giá trị raw, contract đang thực sự được bật hay chưa và đường đi của bản ghi bị loại. |
| A | Cardinality của partition key, số/kích thước file, row-group layout và khả năng sargable của predicate. |
| B | Thứ tự giữa side effect và commit offset, cùng tính idempotent của nơi nhận khi message bị replay. |
