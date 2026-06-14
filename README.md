# Reliable Banking Data Lakehouse Pipeline

Demo pipeline **lakehouse ngân hàng theo hướng streaming-first**: Kafka nhận event giao dịch
thô, Spark Structured Streaming validate/enrich, event hợp lệ được ghi nhận vào PostgreSQL với
bảo đảm **ACID**, đồng thời sinh các CSV curated phục vụ Power BI. Một luồng **batch** được giữ
lại để kiểm thử local và backfill.

> Toàn bộ tài liệu, comment code và file `.md` viết bằng tiếng Việt theo quy ước của repo.

## Mục lục

- [1. Kiến trúc](#1-kiến-trúc)
- [2. Cài đặt & chạy](#2-cài-đặt--chạy)
- [3. Dịch vụ & endpoint](#3-dịch-vụ--endpoint)
- [4. Dữ liệu](#4-dữ-liệu)
- [5. Ghi nhận giao dịch ACID](#5-ghi-nhận-giao-dịch-acid-trên-postgresql)
- [6. Hiệu năng & tối ưu](#6-hiệu-năng--tối-ưu)
- [7. Quan sát (observability) qua log Docker](#7-quan-sát-observability-qua-log-docker)
- [8. Cấu trúc source code](#8-cấu-trúc-source-code)
- [9. Ghi chú Windows / Git Bash](#9-ghi-chú-windows--git-bash)
- [10. Khắc phục sự cố thường gặp](#10-khắc-phục-sự-cố-thường-gặp)

---

## 1. Kiến trúc

### Mục tiêu

1. PostgreSQL lưu dữ liệu master/tham chiếu và **số dư tài khoản**.
2. Kafka nhận event giao dịch thô ở topic `raw-transactions`.
3. Spark Structured Streaming validate và enrich event.
4. Event hợp lệ đi vào `clean-transactions` và `lakehouse/clean/clean_transactions.csv`.
5. Event lỗi đi vào `error-transactions` và `lakehouse/error/error_transactions.csv`.
6. `postgres_transaction_updater` consume clean event và cập nhật số dư trong một DB transaction.
7. Curated/dashboard CSV được xây dựng từ dữ liệu clean trong lakehouse.
8. `export_metrics` xuất CSV cho Power BI và sinh `docs/data_quality_report.md`.

### Luồng xử lý

```text
Transaction Producer
  └─► Kafka raw-transactions
        └─► Spark streaming validator
              ├─► Kafka clean-transactions ─► PostgreSQL updater ─► accounts + ledger_entries
              ├─► Kafka error-transactions
              └─► MinIO lakehouse/{raw, clean, error}
```

Nguyên tắc thiết kế cốt lõi: **Spark sở hữu chất lượng dữ liệu, updater sở hữu trạng thái tài
chính.** Hai phần được tách rời chủ động và chỉ giao tiếp qua Kafka topic.

### Kafka topics

| Topic | Producer | Consumer | Mục đích | Retention |
| --- | --- | --- | --- | --- |
| `raw-transactions` | transaction producer | Spark validator | Event giao dịch thô | 7 ngày |
| `clean-transactions` | Spark validator | PostgreSQL updater | Event hợp lệ, sẵn sàng ghi nhận | 7 ngày |
| `error-transactions` | Spark validator | lakehouse/dashboard | Lỗi chất lượng dữ liệu / nghiệp vụ | 30 ngày |

Mỗi topic có 3 partition (replication factor = 1 vì cụm demo chỉ 1 broker).

### Cấu trúc lakehouse

Bucket MinIO `banking-lakehouse`:

```text
banking-lakehouse/
  lakehouse/
    raw/        raw_transactions.csv
    clean/      clean_transactions.csv
    error/      error_transactions.csv
    curated/    dim_customer.csv, dim_account.csv, dim_card.csv, dim_merchant.csv,
                dim_branch.csv, fact_transaction.csv, fact_loan.csv, fact_repayment.csv,
                daily_transaction_summary.csv, customer_summary.csv
    audit/      (gồm cả Spark checkpoint của streaming validator)
```

Chỉ event giao dịch chảy qua lakehouse. Dữ liệu master/tham chiếu (customers, accounts, cards,
loans, repayments, merchants, branches) nằm trong PostgreSQL / source CSV và **không** mirror lên
MinIO.

---

## 2. Cài đặt & chạy

### Yêu cầu

- Docker Desktop + Docker Compose
- (Tùy chọn) Power BI Desktop để mở dashboard CSV trong `dashboard/`

Không cần cài Python ở host — mọi thứ chạy trong container.

### Chạy demo streaming (đầy đủ)

```bash
sh scripts/start_streaming.sh
```

Script lần lượt: khởi động platform + tạo topic → chạy `pipeline` sinh dữ liệu tham chiếu →
nạp schema/source vào PostgreSQL → bật Spark validator + PostgreSQL updater → bắn transaction
producer → đẩy `lakehouse/` lên MinIO.

### Chạy luồng batch dự phòng

```bash
sh scripts/start.sh
# Quy mô lớn hơn:
CUSTOMERS=10000 TRANSACTIONS=1000000 sh scripts/start.sh
```

### Các script tiện ích

| Script | Tác dụng |
| --- | --- |
| `scripts/setup.sh` | Bật Kafka, Spark, Postgres, MinIO… và tạo 3 topic với retention |
| `scripts/postgres.sh` | Áp `schema.sql` + `load_sources.sql` và in row count |
| `scripts/kafka.sh` | Gửi vài message mẫu vào các topic |
| `scripts/publish.sh` | Mirror `lakehouse/` lên bucket MinIO (profile `tools`) |
| `scripts/dashboard.sh` | Sinh lại các CSV metric cho Power BI |

### Chạy trực tiếp từng stage (PYTHONPATH=src)

```bash
PYTHONPATH=src python -m kafka.generate --customers 1000 --transactions 10000
PYTHONPATH=src python -m application.load_sources_to_lakehouse
PYTHONPATH=src python -m application.prepare_clean_data
PYTHONPATH=src python -m application.build_curated_tables
PYTHONPATH=src python -m application.build_customer_features
PYTHONPATH=src python -m dashboard.export_metrics
```

Các service streaming cũng là CLI module, hỗ trợ `--mode`/`--source`/`--sink` để cùng một code
chạy được cả batch lẫn streaming:

```bash
# Producer: đẩy event vào Kafka (xem thêm --acks, --interval-seconds, --count)
PYTHONPATH=src python -m kafka.producer --sink kafka --topic raw-transactions --count 1000 --interval-seconds 0
# Spark validator
PYTHONPATH=src python -m spark.streaming_validator --mode streaming --bootstrap-servers kafka:9092
# Updater (xem thêm --batch-size)
PYTHONPATH=src python -m application.postgres_transaction_updater --source kafka --topic clean-transactions
```

---

## 3. Dịch vụ & endpoint

| Công cụ | URL | Login |
| --- | --- | --- |
| Kafka UI | `http://localhost:8083` | không cần |
| Spark UI | `http://localhost:8082` | không cần |
| Jupyter | `http://localhost:8888` | không cần |
| MinIO Console | `http://localhost:9001` | `minioadmin` / `minioadmin` |
| pgAdmin | `http://localhost:5050` | `admin@bank.com` / `admin` |
| PostgreSQL | `localhost:5432` | `banking` / `banking` (db `banking`) |
| Power BI input | folder `dashboard/` | — |

---

## 4. Dữ liệu

### Event giao dịch thô

`transaction_id`, `account_id`, `customer_id`, `card_id`, `transaction_time`, `ingestion_time`,
`amount_vnd`, `transaction_type`, `channel`, `merchant_id`, `merchant_category`, `province`,
`currency`, `status`, `source_system`, `producer_ts_ms` (mốc mili-giây để đo end-to-end latency).

### File lakehouse

| Layer | File | Ý nghĩa |
| --- | --- | --- |
| raw | `raw_transactions.csv` | Event nhận vào trước validate |
| clean | `clean_transactions.csv` | Event hợp lệ kèm trường dẫn xuất |
| error | `error_transactions.csv` | Event lỗi kèm metadata lỗi |
| curated | `dim_customer/account/card/merchant/branch.csv` | Các bảng dimension |
| curated | `fact_transaction/loan/repayment.csv` | Các bảng fact phục vụ phân tích |
| curated | `daily_transaction_summary.csv` | Summary theo ngày/kênh/tỉnh cho dashboard |
| curated | `customer_summary.csv` | Feature khách hàng cho credit risk |

### Trường dẫn xuất của giao dịch clean

- `transaction_date`, `transaction_hour` — tách từ `transaction_time`
- `delay_hours` — độ trễ ingestion so với thời điểm giao dịch
- `is_late_arriving` — 1 nếu `delay_hours > 24`
- `is_outlier` — 1 nếu `amount_vnd >= 500.000.000`
- `is_night_transaction` — 1 nếu giờ giao dịch < 6h hoặc >= 22h
- `data_quality_status` — `VALID` hoặc `VALID_WITH_FLAGS`
- `processed_at` — timestamp validate; `batch_id` — ID batch nạp dữ liệu

### Bản ghi error

`error_id`, `source_table`, `raw_payload`, `error_type`, `error_message`, `failed_column`,
`rule_name`, `batch_id`, `source_system`, `ingestion_time`.

### Đầu ra dashboard (`dashboard/`)

| File | Nội dung |
| --- | --- |
| `quality_summary.csv` | Số record valid theo customers/accounts/transactions và tổng invalid |
| `quality_errors.csv` | Thống kê lỗi theo source_table, error_type, cột, rule |
| `storage_benchmark.csv` | Dung lượng (byte) theo layer raw/clean/curated/error |
| `query_benchmark.csv` | Thời gian quét `daily_transaction_summary` |
| `file_inventory.csv` | Danh sách file + row count mỗi lần chạy |
| `daily_transaction_summary.csv` | Copy từ curated để Power BI đọc trực tiếp |

### Báo cáo chất lượng dữ liệu

Mỗi lần chạy pipeline, `export_metrics` tạo `docs/data_quality_report.md` (tự sinh): tóm tắt số
record valid/invalid và bảng chi tiết lỗi theo source_table/error_type/cột/rule.

---

## 5. Ghi nhận giao dịch ACID trên PostgreSQL

`postgres_transaction_updater` consume `clean-transactions` và ghi nhận mỗi giao dịch với:

- **Idempotency** — `processed_transactions.transaction_id` là khóa unique; event trùng bị bỏ qua.
- **Row lock** — `SELECT ... FOR UPDATE` trên `accounts` trước khi đổi số dư.
- **Ledger bất biến** — mỗi giao dịch sinh một dòng `ledger_entries` để audit.
- **Đúng thứ tự** — Kafka offset chỉ commit **sau khi** DB transaction thành công (at-least-once),
  kết hợp idempotency nên an toàn khi reprocess.

Giao dịch không hợp lệ về nghiệp vụ (`ACCOUNT_NOT_FOUND`, `INSUFFICIENT_FUNDS`) được **đánh dấu
REJECTED**, không âm thầm bỏ. Quy ước số tiền là VND nguyên; `transaction_type == "DEPOSIT"` ghi
có (credit), còn lại ghi nợ (debit).

Bảng quan trọng: `processed_transactions` (idempotency + trạng thái), `ledger_entries` (audit),
`transactions` (giao dịch đã posted), `accounts` (số dư, có row lock khi cập nhật).

---

## 6. Hiệu năng & tối ưu

> Đây là project cá nhân chạy trên **một máy** (Docker Desktop/WSL2): 1 Spark worker (1 core/1GB),
> 1 Kafka broker, 1 PostgreSQL node. Các tối ưu dưới đây nhắm vào quy mô đó, không nhằm đạt chỉ
> số của cụm production nhiều node.

### Tối ưu đã áp dụng

- **Producer** — tạo Producer một lần, gom message theo lô (`linger.ms`, `batch.size` lớn, nén
  `lz4`), `acks=all` + idempotence; chỉ flush ở cuối thay vì flush từng message.
- **Spark validator (cache)** — nạp reference (account/customer hợp lệ) **một lần lúc khởi động**
  thay vì đọc lại CSV ở mỗi micro-batch; hạ `spark.sql.shuffle.partitions=8` cho cụm 1 core.
- **PostgreSQL updater (đòn bẩy lớn nhất)** — chuyển từ "1 message = 1 DB transaction = 1 fsync"
  sang **xử lý theo micro-batch: gộp `--batch-size` (mặc định 500) bản ghi vào MỘT transaction,
  chỉ 1 fsync cho cả lô**. Mỗi bản ghi vẫn nằm trong savepoint riêng nên idempotency + row lock +
  ledger giữ nguyên; trong cùng transaction số dư cộng dồn đúng (Postgres đọc được write của chính
  nó). Offset Kafka cũng commit theo lô.
- **Chống deadlock khi chạy nhiều consumer** — vì batch giữ row-lock lâu hơn, mỗi lô được **sắp
  theo `account_id`** (sort ổn định, giữ thứ tự offset trong cùng account) để mọi consumer khóa
  account theo CÙNG thứ tự ⇒ không tạo deadlock cycle; kèm **retry lô** khi vẫn gặp deadlock.

### Số liệu đo (gửi 20.000–30.000 event tức thời)

| Thành phần | Trước tối ưu | Sau tối ưu | Mục tiêu "Tốt" (cụm lớn) |
| --- | --- | --- | --- |
| Producer (1 luồng Python) | ~4.700 msg/s | **~38.000–49.000 msg/s** | 50k–200k |
| Spark validator | ~330–1.130 events/s | **~2.500–3.400 events/s** | 50k–300k |
| Updater **1 instance** | ~400 txn/s | **~1.000–1.069 txn/s** (≈2,5x) | 40k–180k |
| Updater 3 instances | ~735 txn/s | ~1.100 txn/s (≈ 1 instance) | — |

**Tính đúng đắn (ACID)** sau mọi lần chạy: `transactions = ledger_entries = POSTED`; rejection được
đánh dấu; idempotency chống trùng — **không đổi sau khi batch hóa**.

### Phát hiện then chốt

Sau khi batch hóa, **PostgreSQL 1 node trở thành trần throughput (~1.000 txn/s)**: chạy 3 consumer
song song chỉ cho ~1.100 txn/s tổng (≈ bằng 1 instance) do cùng đập vào một DB (tranh chấp row-lock
+ fsync tuần tự). ⇒ **Với máy đơn, 1 updater batched là cấu hình tối ưu** — cũng là default của
`start_streaming.sh`.

Latency end-to-end khi đo lớn (vài chục giây) chủ yếu là **thời gian chờ hàng đợi** do bắn hàng
chục nghìn event tức thời trong khi pipeline rút ~1.000/s — đây là backpressure chứ không phải độ
trễ xử lý thuần. Sàn latency thực tế ≈ thời lượng một micro-batch Spark (vài giây trên 1 core).

### Hướng nâng cấp (nếu mở rộng hạ tầng)

- DB: ghi theo lô bằng `COPY` vào staging rồi MERGE, connection pool, **sharding/partition
  `accounts` theo account_id** để nhiều consumer không tranh chấp.
- Spark: tăng core/executor, dynamic allocation, **bỏ ghi CSV khỏi hot path** (chỉ Kafka/Parquet),
  validate bằng Spark SQL native thay vì collect per-row Python; trigger ngắn để giảm latency.
- Kafka: cụm ≥ 3 broker để dùng replication factor = 3.

---

## 7. Quan sát (observability) qua log Docker

Các service in log thời gian/throughput dạng `key=value`, xem trực tiếp bằng `docker logs`:

```bash
# Producer: tiến độ + throughput
docker logs <producer-container> | grep -E "producer_progress|producer_done"
# Spark: thời lượng + throughput từng micro-batch
docker logs spark-streaming-validator | grep spark_microbatch
# Updater: throughput + end-to-end latency p50/p95/p99
docker logs <updater-container>      | grep updater_throughput
```

---

## 8. Cấu trúc source code

- `src/config/settings.py` — nguồn chân lý duy nhất cho mọi path, default volume, danh sách bảng,
  Kafka servers/topics. Path override được qua env (`DATA_DIR`, `LAKEHOUSE_DIR`, `DASHBOARD_DIR`,
  `DOCS_DIR`).
- `src/kafka/` — `generate.py` (simulator sinh source data), `producer.py` (đẩy event giao dịch ra
  stdout/Kafka/CSV), `inject.py` (chèn lỗi để kiểm thử chất lượng dữ liệu).
- `src/spark/` — `validators.py`, `error_handler.py`, `lakehouse_sink.py`, `streaming_validator.py`.
- `src/application/` — các stage batch (load → clean → curated → customer features) và
  `postgres_transaction_updater.py`.
- `src/connector/` — `postgres.py` (`postgres_config()` → kwargs psycopg3); `minio.py`
  (`minio_config()`, `get_client()`, `upload_file()`; upload hàng loạt dùng `scripts/publish.sh`).
- `src/dashboard/export_metrics.py` — sinh CSV cho Power BI và `docs/data_quality_report.md`.
- `src/utils/` — `io.py` (đọc/ghi CSV, hash, timestamp); `logging.py` (logger `key=value`).
- `configs/resources/postgres/` — `schema.sql`, `load_sources.sql`.

### Quy ước

- Dependency nặng (`pyspark`, `confluent_kafka`, `psycopg`) được import lười qua
  `importlib.util.find_spec` nên module vẫn import được khi thiếu thư viện và chỉ raise
  `RuntimeError` rõ ràng khi thực sự dùng tới. Giữ pattern này khi thêm code chạm các thư viện đó.
- Container `pipeline` dọn `data/`, `lakehouse/` và dashboard CSV mỗi lần chạy trừ khi đặt
  `CLEAN_OUTPUT=0`.

---

## 9. Ghi chú Windows / Git Bash

- Các script `*.sh` được ép **LF** (xem `.gitattributes`) để chạy đúng trong container Linux.
- Trên **Git Bash**, các script đã `export MSYS_NO_PATHCONV=1` để đường dẫn dạng `/opt`, `/sql`
  không bị đổi sang `C:\...` khi truyền vào `docker exec`. Trên Linux/WSL biến này vô hại. Nếu chạy
  lệnh `docker exec ... /opt/...` thủ công trên Git Bash, hãy tự đặt biến này.

---

## 10. Khắc phục sự cố thường gặp

| Triệu chứng | Nguyên nhân | Cách xử lý |
| --- | --- | --- |
| `scripts/...: set: Illegal option -` | Script bị line-ending CRLF | Đã ép LF qua `.gitattributes`; nếu tái diễn, convert lại sang LF |
| Spark validator báo lỗi Kafka connector | Sai version package | Dùng `org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.2` (khớp Spark 4.1.2/Scala 2.13) |
| `FileNotFoundException /nonexistent/.ivy2...` | Spark image không có HOME để ghi cache Ivy | Đã set `--conf spark.jars.ivy=/tmp/.ivy2` + `HOME=/tmp` |
| `mkdir of .../audit/... failed` | User spark không ghi được volume | Validator chạy `user: root` |
| Updater crash kèm `deadlock detected` | Nhiều consumer giữ row-lock lâu, khóa ngược thứ tự | Đã sắp lô theo `account_id` + retry; thêm `restart: unless-stopped` |
