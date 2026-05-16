# Multi-Signal Alert System — Tài liệu module

Hệ thống streaming theo dõi giá cổ phiếu Việt Nam thời gian thực, phân tích **3 chỉ báo kỹ thuật** (VWAP, RSI, Volume), và phát cảnh báo kết hợp khi ≥ 2 tín hiệu đồng thuận. Dữ liệu nguồn từ DNSE WebSocket API, trung chuyển qua Kafka, lưu trữ và phân tích tại ClickHouse, hiển thị trên Streamlit dashboard.

---

## Mục lục

1. [Tổng quan](#1-tổng-quan)
2. [Cấu trúc thư mục](#2-cấu-trúc-thư-mục)
3. [Luồng dữ liệu](#3-luồng-dữ-liệu)
4. [Thành phần chi tiết](#4-thành-phần-chi-tiết)
5. [3 chỉ báo kỹ thuật](#5-ba-chỉ-báo-kỹ-thuật)
6. [Combined Signal Rule](#6-combined-signal-rule)
7. [Schema ClickHouse](#7-schema-clickhouse)
8. [Cấu hình (.env)](#8-cấu-hình-env)
9. [Khởi chạy](#9-khởi-chạy)
10. [Kiểm tra & Debug](#10-kiểm-tra--debug)

---

## 1. Tổng quan

| | |
|---|---|
| **Nguồn dữ liệu** | DNSE WebSocket API (OHLC candles 1 phút) |
| **Message broker** | Apache Kafka (topic `dnse.ohlc`) |
| **Lưu trữ / Phân tích** | ClickHouse (database `vwap`) |
| **Chỉ báo** | VWAP ± σ bands, RSI(14), Volume Spike |
| **Cảnh báo** | Combined Signal Rule — ≥ 2 tín hiệu đồng thuận |
| **Giao diện** | Streamlit dashboard (3-panel chart + alert feed) |
| **Backup** | Parquet → MinIO (thủ công/lập lịch) |

---

## 2. Cấu trúc thư mục

```
vwap_system/
├── producer/                          ── THU DỮ LIỆU ──
│   ├── config.py                      Cấu hình DNSE WS + Kafka
│   ├── ohlc_producer.py               DNSE WebSocket → Kafka (nến 1 phút)
│   └── trade_producer.py              DNSE WebSocket → Kafka (tick, tắt mặc định)
│
├── alert_detector/                    ── XỬ LÝ & CẢNH BÁO ──
│   ├── config.py                      Cấu hình ClickHouse + tham số rules
│   ├── detector.py                    Orchestrator: poll CH → chạy rules → ghi alerts
│   ├── models.py                      Alert dataclass + Severity enums
│   ├── candle_buffer.py               Deque lưu 50 nến gần nhất / symbol
│   ├── vwap.py                        VWAP Calculator (session-based)
│   ├── indicators/                    Hàm tính toán thuần túy (stateless)
│   │   ├── rsi.py                     compute_rsi(closes, period) → float
│   │   └── volume.py                  compute_volume_ratio(volumes, lookback) → float
│   └── rules/                         Plugin cảnh báo (stateful, có cooldown)
│       ├── base.py                    BaseAlertRule abstract class
│       ├── combined_rule.py           ★ CombinedSignalRule (rule chính đang dùng)
│       ├── vwap_rule.py               VWAP rule đơn lẻ (tạm comment)
│       ├── rsi_rule.py                RSI rule đơn lẻ (tạm comment)
│       └── volume_spike_rule.py       Volume rule đơn lẻ (tạm comment)
│
├── dashboard/                         ── HIỂN THỊ ──
│   └── app.py                         Streamlit: 3-panel chart + bảng alerts
│
├── clickhouse/                        ── SCHEMA DATABASE ──
│   └── init.sql                       DDL: ohlc_raw, trades_raw, alerts, alerts_v2
│
├── backup/                            ── LƯU TRỮ DÀI HẠN ──
│   └── minio_exporter.py              ClickHouse → Parquet → MinIO
│
├── .env.example                       Template cấu hình (copy thành .env)
└── VWAP_SYSTEM.md                     Tài liệu này
```

---

## 3. Luồng dữ liệu

```
DNSE WebSocket
      │
      │ subscribe OHLC (nến 1 phút, 5 mã cổ phiếu)
      ▼
┌─────────────────┐      JSON       ┌──────────────┐
│ ohlc_producer.py │ ──────────────→ │ Kafka        │
│ (async, msgpack) │                 │ topic:       │
└─────────────────┘                 │ dnse.ohlc    │
                                     └──────┬───────┘
                                            │ Kafka Engine
                                            │ (auto-consume)
                                            ▼
                                     ┌──────────────┐
                                     │ ClickHouse    │
                                     │ ohlc_raw      │
                                     └──────┬───────┘
                                            │ poll mỗi 10s
                              ┌─────────────┴─────────────┐
                              ▼                           ▼
                       ┌─────────────┐            ┌─────────────┐
                       │ detector.py │            │ dashboard   │
                       │             │            │ app.py      │
                       │ ┌─────────┐ │            │             │
                       │ │VWAP Calc│ │            │ Price+VWAP  │
                       │ │Buffer   │ │            │ RSI chart   │
                       │ │Combined │ │            │ Volume bars │
                       │ │ Rule    │ │            │ Alert table │
                       │ └────┬────┘ │            └─────────────┘
                       │      │      │
                       │      ▼      │
                       │  alerts_v2  │
                       └─────────────┘
```

**Điểm quan trọng:** ClickHouse Kafka Engine hoạt động như consumer tự động — không cần viết code consumer. ClickHouse tự poll Kafka, parse JSON, insert vào bảng MergeTree qua Materialized View.

---

## 4. Thành phần chi tiết

### 4.1 Producer (`producer/ohlc_producer.py`)

- Kết nối DNSE WebSocket qua SDK `TradingClient` (async, encoding msgpack)
- Subscribe nến OHLC resolution 1 phút cho danh sách `SYMBOLS`
- Serialize message → JSON → publish lên Kafka topic `dnse.ohlc`
- Auto-reconnect (tối đa 10 lần), graceful shutdown (SIGINT/SIGTERM)
- Heartbeat log mỗi 60 giây

### 4.2 Detector (`alert_detector/detector.py`)

Orchestrator chính — vòng đời hoạt động:

```
__init__()
  ├── Kết nối ClickHouse
  ├── _ensure_schema()     ← tạo bảng nếu chưa có (idempotent)
  ├── _ensure_alerts_v2()  ← tạo bảng alerts_v2 cho multi-signal
  ├── CandleBuffer(50)     ← khởi tạo buffer rỗng
  ├── CombinedSignalRule() ← đăng ký rule kết hợp
  └── _warm_up()           ← load toàn bộ nến hôm nay từ CH
                              → khôi phục VWAP state + buffer

run() — vòng lặp chính
  └── Mỗi 10 giây (POLL_INTERVAL_SEC):
      ├── Với mỗi symbol:
      │   ├── _fetch_new_ohlc()   → query nến mới hơn watermark
      │   └── _process_candle()   → cập nhật VWAP + buffer + chạy rules
      │       ├── calc.update()      → cập nhật running VWAP
      │       ├── buffer.push()      → đẩy nến vào deque
      │       └── rule.evaluate()    → kiểm tra tổ hợp tín hiệu
      │           └── nếu có alert → _fire_alert() → INSERT alerts_v2
      └── cleanup_old_anchors()   → xóa VWAP state cũ
```

### 4.3 Candle Buffer (`candle_buffer.py`)

- `collections.deque` với `maxlen=50` cho mỗi symbol
- Lưu 50 nến gần nhất (OHLCV + timestamp)
- Cung cấp `get_closes(symbol, n)` cho RSI và `get_volumes(symbol, n)` cho Volume
- Nến cũ nhất tự động bị loại khi buffer đầy

### 4.4 VWAP Calculator (`vwap.py`)

- Tính Session VWAP theo phiên giao dịch (9:00 – 14:45 ICT)
- Công thức: `VWAP = Σ(typical_price × volume) / Σ(volume)`
- `typical_price = (high + low + close) / 3`
- Tính σ (volume-weighted standard deviation) cho bands
- Reset mỗi đầu ngày mới (session-based)

### 4.5 Dashboard (`dashboard/app.py`)

Streamlit web app với 3 panel:

| Panel | Nội dung |
|-------|----------|
| **Panel 1** (55%) | Biểu đồ giá (Price line) + đường VWAP (dash) + σ-bands (filled area) |
| **Panel 2** (20%) | RSI(14) với đường 70 (quá mua, đỏ) và 30 (quá bán, xanh) |
| **Panel 3** (25%) | Volume bars — cột đỏ khi spike ≥ 3x trung bình, đường Vol Avg(20) |

Sidebar: chọn mã, thời gian hiển thị, lọc alert theo rule/severity.
Metrics row: Candles hôm nay, Alerts hôm nay, Giá hiện tại, RSI, Volume ratio, VWAP.

---

## 5. Ba chỉ báo kỹ thuật

### 5.1 VWAP — Volume-Weighted Average Price

**Câu hỏi:** Giá hiện tại ở đâu so với giá trị hợp lý trong phiên?

```
VWAP  = Σ(typical_price × volume) / Σ(volume)
σ     = √(Σ(tp² × vol) / Σ(vol) − VWAP²)
upper = VWAP + k × σ     (mặc định k=2.0)
lower = VWAP − k × σ
```

- Giá > upper → **Breakout** (giá cao bất thường so với phiên)
- Giá < lower → **Breakdown** (giá thấp bất thường)
- Tính bằng running sum (O(1) mỗi nến, không cần lưu toàn bộ lịch sử)

### 5.2 RSI — Relative Strength Index

**Câu hỏi:** Đà tăng/giảm còn bền vững không?

```
RSI = 100 − 100 / (1 + RS)
RS  = avg_gain(14) / avg_loss(14)
```

- RSI > 70 → **Quá mua** (momentum cạn kiệt, rủi ro giảm)
- RSI < 30 → **Quá bán** (có thể đảo chiều tăng)
- Cần ít nhất 15 nến close để tính (period + 1)

### 5.3 Volume Spike — Khối lượng đột biến

**Câu hỏi:** Có "big money" đang hoạt động bất thường?

```
volume_ratio = current_volume / avg(volume, 20 nến trước)
```

- Ratio ≥ 3.0 → **Spike** (khối lượng gấp 3 trung bình)
- Cần ít nhất 21 nến volume (lookback + 1)

---

## 6. Combined Signal Rule

**Đây là rule chính đang hoạt động.** Thay vì 3 rule chạy rời rạc, CombinedSignalRule kiểm tra tổ hợp cả 3 chỉ báo và chỉ phát cảnh báo khi ≥ 2 tín hiệu đồng thuận.

### Bảng ma trận cảnh báo

| VWAP | RSI | Volume | Alert Type | Severity | Ý nghĩa |
|------|-----|--------|------------|----------|---------|
| Breakout ↑ | > 70 | Spike ≥ 3x | `COMBINED_PUMP_RISK` | **CRITICAL** | ⚠️ Rủi ro đẩy giá — cả 3 tín hiệu đều cực đoan |
| Breakdown ↓ | < 30 | Spike ≥ 3x | `COMBINED_PANIC_SELL` | **CRITICAL** | 🔴 Bán tháo — panic selling |
| Breakout ↑ | > 70 | Bình thường | `COMBINED_OVERBOUGHT_BREAKOUT` | WARNING | Breakout + quá mua — cẩn trọng |
| Breakdown ↓ | < 30 | Bình thường | `COMBINED_OVERSOLD_BREAKDOWN` | WARNING | Breakdown + quá bán — có thể là cơ hội |
| Trong band | 30-70 | Spike ≥ 3x | `COMBINED_UNUSUAL_VOLUME` | WARNING | 🟠 KL bất thường — đang có tin? |
| Breakout/Down | 30-70 | Spike ≥ 3x | `COMBINED_VOLUME_BREAKOUT/BREAKDOWN` | WARNING | Giá lệch VWAP + KL lớn |

### Cooldown

Mỗi alert type có cooldown **5 phút** (mặc định) cho mỗi symbol:
- HPG báo `COMBINED_PUMP_RISK` lúc 10:00 → không báo lại cho HPG đến 10:05
- SSI vẫn có thể báo độc lập (cooldown theo từng symbol)

### Tại sao dùng Combined thay vì 3 rule riêng?

| | 3 Rule riêng | Combined Rule |
|---|---|---|
| Số alert | Nhiều, dễ spam | Ít, chất lượng cao |
| False alarm | Cao | Thấp (cần ≥ 2 đồng thuận) |
| Ý nghĩa | Phải tự ghép | Máy kết luận sẵn |

---

## 7. Schema ClickHouse

Database: `vwap`

| Bảng | Engine | TTL | Mục đích |
|------|--------|-----|----------|
| `ohlc_raw` | MergeTree | 90 ngày | Nến OHLCV 1 phút (từ Kafka) |
| `kafka_ohlc` | Kafka | — | Cổng nhận message Kafka |
| `kafka_to_ohlc_raw` | MV | — | Tự động parse + insert |
| `trades_raw` | MergeTree | 90 ngày | Tick giao dịch (tắt mặc định) |
| `kafka_trades` | Kafka | — | Cổng nhận trades |
| `kafka_to_trades_raw` | MV | — | Tự động parse + insert |
| `alerts` | MergeTree | 90 ngày | Alerts cũ (chỉ VWAP, tương thích ngược) |
| **`alerts_v2`** | MergeTree | 90 ngày | **Alerts mới** — multi-signal |

### Schema `alerts_v2`

```sql
CREATE TABLE vwap.alerts_v2 (
    alert_time      DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol          LowCardinality(String),
    rule_name       LowCardinality(String),    -- COMBINED
    alert_type      String,                    -- COMBINED_PUMP_RISK, ...
    severity        LowCardinality(String),    -- WARNING, CRITICAL
    price           Float64,
    indicator_value Float64,                   -- RSI hoặc volume ratio
    threshold       Float64,
    deviation_pct   Float64,                   -- % lệch giá vs VWAP
    message         String                     -- Mô tả tiếng Việt
) ENGINE = MergeTree()
ORDER BY (alert_time, symbol, rule_name)
TTL toDate(alert_time) + INTERVAL 90 DAY;
```

---

## 8. Cấu hình (.env)

File `.env` đặt ở **thư mục gốc repo** (`DATN_project/.env`). Template: `vwap_system/.env.example`.

### Các biến quan trọng

| Nhóm | Biến | Mặc định | Mô tả |
|------|------|----------|-------|
| **DNSE** | `DNSE_API_KEY` | *(bắt buộc)* | API key từ DNSE Open API |
| | `DNSE_API_SECRET` | *(bắt buộc)* | API secret |
| **Kafka** | `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| **ClickHouse** | `CLICKHOUSE_HOST` | `localhost` | ClickHouse host |
| | `CLICKHOUSE_HTTP_PORT` | `8123` | HTTP port |
| **VWAP** | `ALERT_BAND_MODE` | `sigma` | `sigma` hoặc `pct` |
| | `BAND_SIGMA_MULTIPLIER` | `2.0` | Hệ số k cho bands |
| **RSI** | `RSI_PERIOD` | `14` | Chu kỳ RSI |
| | `RSI_OVERBOUGHT` | `70` | Ngưỡng quá mua |
| | `RSI_OVERSOLD` | `30` | Ngưỡng quá bán |
| **Volume** | `VOLUME_LOOKBACK` | `20` | Số nến tính trung bình |
| | `VOLUME_SPIKE_RATIO` | `3.0` | Ngưỡng spike |
| **Runtime** | `POLL_INTERVAL_SEC` | `10` | Chu kỳ poll ClickHouse |
| | `ALERT_COOLDOWN_SEC` | `300` | Cooldown giữa alerts (giây) |
| | `CANDLE_BUFFER_SIZE` | `50` | Số nến giữ trong buffer |
| | `SYMBOLS` | `HPG,SSI,VNM,VCB,TCB` | Danh sách mã theo dõi |

---

## 9. Khởi chạy

### Yêu cầu
- Docker + Docker Compose
- Python 3.11+
- uv (package manager)
- Tài khoản DNSE Open API (có API key/secret)

### Bước 1: Hạ tầng Docker

```bash
cd /Users/buihung/DATN_Huy/DATN_project

# Bật ClickHouse + MinIO + Kafka (profile vwap)
docker compose --profile vwap up -d

# Kiểm tra
docker compose ps
```

### Bước 2: Cài đặt dependencies

```bash
uv sync --extra vwap
```

### Bước 3: Cấu hình .env

```bash
# Tạo file .env từ template
cp vwap_system/.env.example .env

# Sửa .env — điền DNSE credentials thật
# DNSE_API_KEY=your-real-key
# DNSE_API_SECRET=your-real-secret
```

### Bước 4: Chạy 3 terminal

```bash
# Terminal 1: Producer (thu dữ liệu DNSE → Kafka)
uv run vwap_system/producer/ohlc_producer.py

# Terminal 2: Detector (phân tích + cảnh báo)
uv run vwap_system/alert_detector/detector.py

# Terminal 3: Dashboard (giao diện web)
uv run streamlit run vwap_system/dashboard/app.py
```

### Bước 5: Xem kết quả

- **Dashboard:** http://localhost:8501
- **Kafka UI:** http://localhost:8080 (nếu bật)
- **MinIO Console:** http://localhost:9101

> **Lưu ý:** Producer chỉ nhận dữ liệu trong giờ giao dịch (9:00 – 14:45 ICT, Thứ 2 – Thứ 6). Ngoài giờ sẽ không có nến mới.

---

## 10. Kiểm tra & Debug

### Kiểm tra dữ liệu trong ClickHouse

```bash
# Số nến hôm nay
docker compose exec clickhouse-01 clickhouse-client \
  --user default --password default \
  -q "SELECT count() FROM vwap.ohlc_raw WHERE toDate(candle_time) = today()"

# Xem 5 nến gần nhất
docker compose exec clickhouse-01 clickhouse-client \
  --user default --password default \
  -q "SELECT candle_time, symbol, close, volume FROM vwap.ohlc_raw ORDER BY candle_time DESC LIMIT 5"

# Xem alerts kết hợp
docker compose exec clickhouse-01 clickhouse-client \
  --user default --password default \
  -q "SELECT alert_time, symbol, alert_type, severity, message FROM vwap.alerts_v2 ORDER BY alert_time DESC LIMIT 10"
```

### Log output mẫu

**Producer:**
```
2026-05-12 10:00:15 [INFO] ohlc_producer: Kafka producer ready → localhost:9092
2026-05-12 10:00:16 [INFO] ohlc_producer: Connected to DNSE. Subscribing: ['HPG', 'SSI', ...]
2026-05-12 10:01:15 [INFO] ohlc_producer: [HPG] 1m close=26.75 vol=150,000 total_sent=200
```

**Detector:**
```
2026-05-12 10:00:20 [INFO] detector: Registered rules: ['COMBINED']
2026-05-12 10:00:21 [INFO] detector: Warm-up done: 245 candles loaded
2026-05-12 10:05:30 [WARNING] detector: 🚨 [COMBINED] COMBINED_PUMP_RISK | HPG
    price=27.50 indicator=78.00 severity=CRITICAL
    | HPG ⚠️ RỦI RO ĐẨY GIÁ — Breakout VWAP + RSI=78 (quá mua) + KL 4.2x
```

### Bật lại rule đơn lẻ (nếu cần)

Mở `alert_detector/detector.py`, bỏ comment các dòng 21-23 và 51-55 để chạy song song 3 rule riêng lẻ bên cạnh Combined rule.
