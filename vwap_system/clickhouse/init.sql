-- =============================================================
-- VWAP Alert System — ClickHouse Schema
-- File này tự động chạy khi container khởi động lần đầu
-- =============================================================

-- Tạo database
CREATE DATABASE IF NOT EXISTS vwap;

-- ---------------------------------------------------------------
-- 1. trades_raw: lưu toàn bộ tick giao dịch (nguồn sự thật)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vwap.trades_raw
(
    received_at  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol       LowCardinality(String),
    price        Float64,
    quantity     Int64,
    total_volume Int64,
    board_id     Int16,
    market_id    Int16
)
ENGINE = MergeTree()
PARTITION BY toDate(received_at)
ORDER BY (symbol, received_at)
TTL toDate(received_at) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------
-- 2. kafka_trades: Kafka Engine — cổng nhận message từ Kafka
--    Chú ý: broker dùng tên service Docker (kafka:29092)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vwap.kafka_trades
(
    received_at  String,
    symbol       String,
    price        Float64,
    quantity     Int64,
    total_volume Int64,
    board_id     Int16,
    market_id    Int16
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'dnse.trades',
    kafka_group_name         = 'clickhouse_vwap_consumer',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 1,
    kafka_max_block_size     = 65536,
    kafka_skip_broken_messages = 10;

-- ---------------------------------------------------------------
-- 3. Materialized View: tự động chuyển kafka_trades → trades_raw
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS vwap.kafka_to_trades_raw
TO vwap.trades_raw
AS
SELECT
    parseDateTime64BestEffort(received_at, 3, 'Asia/Ho_Chi_Minh') AS received_at,
    symbol,
    price,
    quantity,
    total_volume,
    board_id,
    market_id
FROM vwap.kafka_trades;

-- ---------------------------------------------------------------
-- 5. ohlc_raw: lưu nến OHLCV (resolution ~ 1 phút) từ websocket
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vwap.ohlc_raw
(
    received_at  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    candle_time  DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol       LowCardinality(String),
    resolution   String,
    market_type  String,
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Int64,
    lastUpdated   Int64
)
ENGINE = MergeTree()
PARTITION BY toDate(candle_time)
ORDER BY (symbol, candle_time)
TTL toDate(received_at) + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- ---------------------------------------------------------------
-- 6. kafka_ohlc: Kafka Engine — cổng nhận OHLC message
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vwap.kafka_ohlc
(
    received_at  String,
    symbol       String,
    resolution   String,
    open          Float64,
    high          Float64,
    low           Float64,
    close         Float64,
    volume        Int64,
    type          String,
    time          UInt32,
    lastUpdated   UInt32
)
ENGINE = Kafka
SETTINGS
    kafka_broker_list        = 'kafka:29092',
    kafka_topic_list         = 'dnse.ohlc',
    kafka_group_name         = 'clickhouse_vwap_consumer_ohlc',
    kafka_format             = 'JSONEachRow',
    kafka_num_consumers      = 1,
    kafka_max_block_size     = 65536,
    kafka_skip_broken_messages = 10;

-- ---------------------------------------------------------------
-- 7. Materialized View: tự động chuyển kafka_ohlc → ohlc_raw
-- ---------------------------------------------------------------
CREATE MATERIALIZED VIEW IF NOT EXISTS vwap.kafka_to_ohlc_raw
TO vwap.ohlc_raw
AS
SELECT
    parseDateTime64BestEffort(received_at, 3, 'Asia/Ho_Chi_Minh') AS received_at,
    toDateTime64(toDateTime(time), 3, 'Asia/Ho_Chi_Minh') AS candle_time,
    symbol,
    resolution,
    type AS market_type,
    open,
    high,
    low,
    close,
    volume,
    lastUpdated
FROM vwap.kafka_ohlc;

-- ---------------------------------------------------------------
-- 4. alerts: lưu cảnh báo VWAP do detector ghi vào
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vwap.alerts
(
    alert_time    DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol        LowCardinality(String),
    alert_type    String,          -- BREAKOUT_UP | BREAKDOWN
    price         Float64,
    vwap          Float64,
    deviation_pct Float64
)
ENGINE = MergeTree()
ORDER BY (alert_time, symbol)
TTL toDate(alert_time) + INTERVAL 90 DAY;

-- ---------------------------------------------------------------
-- 8. alerts_v2: multi-signal alerts (VWAP, RSI, Volume Spike)
-- ---------------------------------------------------------------
CREATE TABLE IF NOT EXISTS vwap.alerts_v2
(
    alert_time      DateTime64(3, 'Asia/Ho_Chi_Minh'),
    symbol          LowCardinality(String),
    rule_name       LowCardinality(String),    -- VWAP, RSI, VOLUME_SPIKE
    alert_type      String,                    -- VWAP_BREAKOUT_UP, RSI_OVERBOUGHT, ...
    severity        LowCardinality(String),    -- INFO, WARNING, CRITICAL
    price           Float64,
    indicator_value Float64,                   -- Giá trị chỉ báo lúc trigger
    threshold       Float64,                   -- Ngưỡng đã dùng
    deviation_pct   Float64,                   -- % lệch (tương thích cũ)
    message         String                     -- Mô tả tiếng Việt
)
ENGINE = MergeTree()
ORDER BY (alert_time, symbol, rule_name)
TTL toDate(alert_time) + INTERVAL 90 DAY;
