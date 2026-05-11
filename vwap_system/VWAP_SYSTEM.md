# VWAP Alert System — Tài liệu module

Hệ thống streaming theo dõi giá cổ phiếu thời gian thực, tính **Volume-Weighted Average Price (VWAP)** theo phiên, phát hiện breakout/breakdown và hiển thị cảnh báo trực tiếp trên dashboard Streamlit. Dữ liệu nguồn từ DNSE WebSocket API, trung chuyển qua Kafka, lưu trữ tại ClickHouse.

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Luồng dữ liệu](#3-luồng-dữ-liệu)
4. [Thành phần chi tiết](#4-thành-phần-chi-tiết)
5. [Schema ClickHouse](#5-schema-clickhouse)
6. [Cấu hình (.env)](#6-cấu-hình-env)
7. [Docker Compose](#7-docker-compose)
8. [Khởi chạy](#8-khởi-chạy)
9. [Lưu ý bảo mật](#9-lưu-ý-bảo-mật)

---

## 1. Tổng quan

| | |
|---|---|
| **Nguồn dữ liệu** | DNSE WebSocket API (OHLC candles + tick giao dịch) |
| **Message broker** | Apache Kafka (2 topics) |
| **Lưu trữ** | ClickHouse (database `vwap`, 3 bảng MergeTree) |
| **Phát hiện cảnh báo** | Session VWAP ± sigma bands hoặc % threshold |
| **Giao diện** | Streamlit dashboard (auto-refresh) |
| **Backup** | Parquet → MinIO (hàng ngày) |
| **TTL dữ liệu** | 90 ngày trên tất cả bảng raw |

---

## 2. Cấu trúc thư mục

```
vwap_system/
├── .env.example                  # Template cấu hình môi trường
├── clickhouse/
│   └── init.sql                  # DDL schema ClickHouse (tự chạy khi container khởi động)
├── producer/
│   ├── config.py                 # Đọc .env, expose cấu hình Kafka + DNSE
│   ├── ohlc_producer.py          # DNSE WS → Kafka topic dnse.ohlc  [mặc định BẬT]
│   └── trade_producer.py         # DNSE WS → Kafka topic dnse.trades [mặc định TẮT]
├── alert_detector/
│   ├── config.py                 # Đọc .env, expose cấu hình ClickHouse + alert
│   ├── vwap.py                   # VWAPCalculator: tính session VWAP + sigma bands
│   └── detector.py               # AlertDetector: poll CH → tính VWAP → INSERT alerts
├── dashboard/
│   └── app.py                    # Streamlit UI: biểu đồ giá/VWAP/bands + bảng cảnh báo
├── backup/
│   └── minio_exporter.py         # Export trades_raw + alerts → Parquet → MinIO
├── trade_realtime.py             # Demo subscribe tick (chứa hardcoded credentials)
└── trade_realtime_template.py    # Demo subscribe OHLC (chứa hardcoded credentials)
```

---

## 3. Luồng dữ liệu

```
                        ┌─────────────────────────────────────────────────────┐
                        │                   DNSE WebSocket                    │
                        └──────────────┬──────────────────┬───────────────────┘
                                       │                  │ (ENABLE_TRADE_PIPELINE=1)
                                       ▼                  ▼
                              ohlc_producer        trade_producer *
                                       │                  │
                                       ▼                  ▼
                             Kafka: dnse.ohlc    Kafka: dnse.trades
                                       │                  │
                          [Kafka Engine + MV]  [Kafka Engine + MV]
                                       │                  │
                                       ▼                  ▼
                                   ohlc_raw          trades_raw
                                  (MergeTree)        (MergeTree)
                                       │                  │
                              ┌────────┤                  ├──────────┐
                              │        │                  │          │
                              ▼        ▼                  ▼          ▼
                          Dashboard  alert_detector    MinIO Backup
                         (Streamlit)      │
                              ▲          ▼
                              │       alerts
                              │      (MergeTree)
                              └──────────┘
```

**Pipeline chính (OHLC):** DNSE WS → `ohlc_producer` → Kafka `dnse.ohlc` → ClickHouse Kafka Engine → Materialized View → `ohlc_raw` → `alert_detector` → `alerts`

**Pipeline tick (tuỳ chọn):** DNSE WS → `trade_producer` → Kafka `dnse.trades` → ClickHouse Kafka Engine → Materialized View → `trades_raw`

**Consumers đọc:** `Dashboard` đọc `ohlc_raw` + `alerts`; `MinIO Exporter` đọc `trades_raw` + `alerts`.

> `*` trade_producer và toàn bộ pipeline tick chỉ hoạt động khi `ENABLE_TRADE_PIPELINE=1`.

---

## 4. Thành phần chi tiết

### 4.1 `producer/ohlc_producer.py` — OHLC Producer

- Khởi tạo DNSE `TradingClient` với credentials từ `.env`.
- Subscribe OHLC candles (`subscribe_ohlc`) cho từng symbol trong danh sách `SYMBOLS`.
- Serialize message thành JSON, publish lên Kafka topic `KAFKA_OHLC_TOPIC` (`dnse.ohlc`).
- **Chạy mặc định**, không cần flag đặc biệt.

### 4.2 `producer/trade_producer.py` — Trade Producer _(tuỳ chọn)_

- Subscribe tick giao dịch (`subscribe_trade`) từ DNSE WebSocket.
- Publish JSON lên Kafka topic `KAFKA_TOPIC` (`dnse.trades`).
- **Tắt mặc định** — chỉ chạy khi `ENABLE_TRADE_PIPELINE=1` (early-exit guard trong `main()`).

### 4.3 `alert_detector/vwap.py` — VWAPCalculator

Tính **session VWAP** theo công thức:

```
VWAP = Σ(typical_price × volume) / Σvolume

typical_price = (High + Low + Close) / 3
```

- Khung giờ giao dịch: **09:00 – 14:45 ICT** (Asia/Ho_Chi_Minh).
- Duy trì **volume-weighted sigma** (σ) để tính upper/lower bands.
- Reset VWAP về 0 đầu mỗi phiên giao dịch.

### 4.4 `alert_detector/detector.py` — AlertDetector

Vòng lặp chính:

1. **Warm-up**: load toàn bộ candles trong ngày hôm nay từ `ohlc_raw` vào `VWAPCalculator`.
2. **Poll loop** mỗi `POLL_INTERVAL_SEC` giây:
   - Query `ohlc_raw` lấy các candle mới hơn watermark.
   - Cập nhật `VWAPCalculator` cho từng symbol.
   - So sánh giá với VWAP band:
     - **`pct` mode**: `|price − vwap| / vwap > ALERT_THRESHOLD_PCT%`
     - **`sigma` mode**: `price > vwap + k×σ` hoặc `price < vwap − k×σ`
   - INSERT cảnh báo `BREAKOUT_UP` / `BREAKDOWN` vào bảng `alerts`.

### 4.5 `dashboard/app.py` — Streamlit Dashboard

- Kết nối ClickHouse qua HTTP interface.
- Tính VWAP + σ-bands bằng **SQL window functions** trên `ohlc_raw`.
- Hiển thị biểu đồ **Plotly**: đường giá đóng cửa, đường VWAP, upper/lower bands.
- Bảng cảnh báo gần nhất từ `alerts`.
- **Auto-refresh** mỗi `DASHBOARD_REFRESH_SEC` giây (mặc định 5s).

### 4.6 `backup/minio_exporter.py` — MinIO Exporter

- Nhận ngày cần export qua CLI arg (mặc định = hôm nay theo ICT).
- Query `trades_raw` và `alerts` theo ngày.
- Ghi Parquet (Snappy compress) lên MinIO:
  - `trades/date=YYYY-MM-DD/data.parquet`
  - `alerts/date=YYYY-MM-DD/data.parquet`

---

## 5. Schema ClickHouse

Database: **`vwap`** — khởi tạo tự động qua `clickhouse/init.sql` khi container lần đầu start.

### Bảng lưu trữ (MergeTree)

#### `vwap.trades_raw`
```sql
CREATE TABLE vwap.trades_raw (
    received_at  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol       LowCardinality(String),
    price        Float64,
    quantity     Int64,
    total_volume Int64,
    board_id     Int16,
    market_id    Int16
) ENGINE = MergeTree()
PARTITION BY toDate(received_at)
ORDER BY (symbol, received_at)
TTL toDate(received_at) + INTERVAL 90 DAY;
```

#### `vwap.ohlc_raw`
```sql
CREATE TABLE vwap.ohlc_raw (
    received_at  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    candle_time  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol       LowCardinality(String),
    resolution   String,
    market_type  String,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       Int64,
    lastUpdated  Int64
) ENGINE = MergeTree()
PARTITION BY toDate(candle_time)
ORDER BY (symbol, candle_time)
TTL toDate(received_at) + INTERVAL 90 DAY;
```

#### `vwap.alerts`
```sql
CREATE TABLE vwap.alerts (
    alert_time    DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol        LowCardinality(String),
    alert_type    String,        -- BREAKOUT_UP | BREAKDOWN
    price         Float64,
    vwap          Float64,
    deviation_pct Float64
) ENGINE = MergeTree()
ORDER BY (alert_time, symbol)
TTL toDate(alert_time) + INTERVAL 90 DAY;
```

### Kafka Engines (cổng nhận dữ liệu từ Kafka)

| Bảng | Topic | Consumer Group |
|------|-------|----------------|
| `kafka_trades` | `dnse.trades` | `clickhouse_vwap_consumer` |
| `kafka_ohlc` | `dnse.ohlc` | `clickhouse_vwap_consumer_ohlc` |

Broker (bên trong Docker): `kafka:29092`

### Materialized Views (chuyển dữ liệu tự động)

| View | Từ | Đến | Xử lý |
|------|-----|-----|-------|
| `kafka_to_trades_raw` | `kafka_trades` | `trades_raw` | Parse ISO timestamp → DateTime64 |
| `kafka_to_ohlc_raw` | `kafka_ohlc` | `ohlc_raw` | Convert Unix epoch → DateTime64, map `type` → `market_type` |

---

## 6. Cấu hình (.env)

Copy template và điền giá trị thực:

```bash
cp vwap_system/.env.example .env
```

> Tất cả service đọc `.env` từ **thư mục gốc của repo** (không phải trong `vwap_system/`).

### DNSE API

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `DNSE_API_KEY` | _(bắt buộc)_ | API key xác thực DNSE |
| `DNSE_API_SECRET` | _(bắt buộc)_ | API secret xác thực DNSE |
| `DNSE_WS_URL` | `wss://ws-openapi.dnse.com.vn` | WebSocket endpoint của DNSE Open API |
| `DNSE_ENCODING` | `msgpack` | Định dạng encoding frame WebSocket |

### Kafka

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Broker address. Từ host dùng `:9092`; trong Docker dùng `kafka:29092` |
| `KAFKA_TOPIC` | `dnse.trades` | Topic cho tick giao dịch |
| `KAFKA_OHLC_TOPIC` | `dnse.ohlc` | Topic cho nến OHLC |

### ClickHouse

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `CLICKHOUSE_HOST` | `localhost` | Host ClickHouse HTTP interface |
| `CLICKHOUSE_HTTP_PORT` | `8123` | Port HTTP interface |
| `CLICKHOUSE_USER` | `default` | User ClickHouse |
| `CLICKHOUSE_PASSWORD` | `default` | Password ClickHouse |
| `CLICKHOUSE_DB` | `vwap` | Database |

### MinIO

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `MINIO_ENDPOINT` | `localhost:9100` | MinIO endpoint |
| `MINIO_ACCESS_KEY` | `minio_access_key` | Access key |
| `MINIO_SECRET_KEY` | `minio_secret_key` | Secret key |
| `MINIO_BUCKET` | `stock-data` | Bucket đích cho backup |

### Alert & Runtime

| Biến | Mặc định | Mô tả |
|------|----------|-------|
| `SYMBOLS` | `HPG,SSI,VNM,VCB,TCB` | Danh sách mã cổ phiếu theo dõi (phân cách dấu phẩy) |
| `OHLC_RESOLUTION` | `1` | Độ phân giải nến (phút) |
| `ALERT_BAND_MODE` | `sigma` | `pct` = % lệch khỏi VWAP \| `sigma` = ± k × σ |
| `ALERT_THRESHOLD_PCT` | `1.5` | Ngưỡng % khi `ALERT_BAND_MODE=pct` |
| `BAND_SIGMA_MULTIPLIER` | `2.0` | Hệ số k khi `ALERT_BAND_MODE=sigma` |
| `POLL_INTERVAL_SEC` | `10` | Chu kỳ poll `ohlc_raw` của detector (giây) |
| `ENABLE_TRADE_PIPELINE` | `0` | Đặt `=1` để bật pipeline tick giao dịch |
| `DASHBOARD_REFRESH_SEC` | `5` | Chu kỳ auto-refresh của Streamlit dashboard (giây) |

---

## 7. Docker Compose

VWAP system tích hợp vào file `docker-compose.yml` ở gốc repo.

### Khởi động ClickHouse (luôn cần)

```bash
docker compose up -d clickhouse-01
```

- Mount `vwap_system/clickhouse/init.sql` → `/docker-entrypoint-initdb.d/02_vwap_init.sql`
- Schema được tạo tự động khi container khởi động lần đầu.
- Chỉ node `clickhouse-01` chạy Kafka consumer (tránh duplicate consumption trên replicas).

### Khởi động Kafka stack (profile `vwap`)

```bash
docker compose --profile vwap up -d
```

| Service | Hostname | Port nội bộ | Port từ host |
|---------|----------|-------------|--------------|
| `vwap-zookeeper` | `zookeeper` | 2181 | 2181 |
| `vwap-kafka` | `kafka` | 29092 (INTERNAL) | 9092 (EXTERNAL) |
| `vwap-kafka-ui` | — | — | 8080 |

> ClickHouse dùng `kafka:29092` (internal listener). Producer từ host dùng `localhost:9092`.

### MinIO

Dùng chung MinIO của project (`localhost:9100`), không cần khởi động riêng.

---

## 8. Khởi chạy

### Bước 1 — Cài dependencies

```bash
pip install -e ".[vwap]"
```

### Bước 2 — Khởi động hạ tầng

```bash
# ClickHouse
docker compose up -d clickhouse-01

# Kafka stack
docker compose --profile vwap up -d
```

### Bước 3 — Cấu hình môi trường

```bash
cp vwap_system/.env.example .env
# Chỉnh sửa .env với DNSE_API_KEY, DNSE_API_SECRET và các giá trị thực
```

### Bước 4 — Chạy các service

Mở các terminal riêng biệt:

```bash
# Terminal 1 — OHLC Producer
python -m vwap_system.producer.ohlc_producer

# Terminal 2 — Alert Detector
python -m vwap_system.alert_detector.detector

# Terminal 3 — Dashboard
streamlit run vwap_system/dashboard/app.py

# [Tuỳ chọn] Terminal 4 — Tick Producer
ENABLE_TRADE_PIPELINE=1 python -m vwap_system.producer.trade_producer
```

### Bước 5 — Backup thủ công (tuỳ chọn)

```bash
# Export ngày hôm nay
python -m vwap_system.backup.minio_exporter

# Export ngày cụ thể
python -m vwap_system.backup.minio_exporter 2026-05-10
```

---

## 9. Lưu ý bảo mật

> **Credentials bị hardcode trong source code**

Các file sau chứa DNSE API key/secret được nhúng trực tiếp vào code:

- `vwap_system/trade_realtime.py`
- `vwap_system/trade_realtime_template.py`

**Cần thực hiện trước khi commit hoặc deploy:**

1. Rotate API key/secret trên portal DNSE ngay lập tức nếu đã push lên remote.
2. Xóa credentials hardcode khỏi hai file trên, thay bằng `os.getenv("DNSE_API_KEY")`.
3. Kiểm tra `.env` thực (có credentials) đã nằm trong `.gitignore` — không được commit vào repo.
