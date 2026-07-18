-- =============================================================
-- stock_data: DDL cho ClickHouse Cluster 2S2R
-- =============================================================
-- Cluster: cluster_2s2r (2 Shard × 2 Replica)
-- Pattern: ReplicatedReplacingMergeTree (local) + Distributed (query)
-- =============================================================

CREATE DATABASE IF NOT EXISTS stock_data ON CLUSTER cluster_2s2r;

-- ╔═══════════════════════════════════════════════════════════════╗
-- ║                        DIMENSIONS                           ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE TABLE IF NOT EXISTS stock_data.dim_stock_local ON CLUSTER cluster_2s2r (
    stock_key       Int32,
    symbol          String,
    name            Nullable(String),
    sector          Nullable(String),
    profile         Nullable(String),
    listing_date    Nullable(String),
    issued_share    Nullable(Float64),
    exchange        Nullable(String),
    type            Nullable(String),
    organ_short_name Nullable(String),
    organ_name      Nullable(String)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/dim_stock',
    '{replica}'
)
ORDER BY (stock_key);

CREATE TABLE IF NOT EXISTS stock_data.dim_stock ON CLUSTER cluster_2s2r
AS stock_data.dim_stock_local
ENGINE = Distributed(cluster_2s2r, stock_data, dim_stock_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.dim_index_local ON CLUSTER cluster_2s2r (
    index_key       Int64,
    index_symbol    String,
    index_name      Nullable(String),
    description     Nullable(String),
    `group`         Nullable(String)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/dim_index',
    '{replica}'
)
ORDER BY (index_key);

CREATE TABLE IF NOT EXISTS stock_data.dim_index ON CLUSTER cluster_2s2r
AS stock_data.dim_index_local
ENGINE = Distributed(cluster_2s2r, stock_data, dim_index_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.dim_date_event_local ON CLUSTER cluster_2s2r (
    date_key        Int32,
    trade_date      Date,
    cal_year        Nullable(Int32),
    cal_quarter     Nullable(Int32),
    cal_month       Nullable(Int32),
    cal_week        Nullable(Int32),
    event_name      Nullable(String),
    is_day_off      Nullable(UInt8)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/dim_date_event',
    '{replica}'
)
ORDER BY (date_key);

CREATE TABLE IF NOT EXISTS stock_data.dim_date_event ON CLUSTER cluster_2s2r
AS stock_data.dim_date_event_local
ENGINE = Distributed(cluster_2s2r, stock_data, dim_date_event_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.dim_sector_local ON CLUSTER cluster_2s2r (
    sector_key      Int32,
    sector          String
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/dim_sector',
    '{replica}'
)
ORDER BY (sector_key);

CREATE TABLE IF NOT EXISTS stock_data.dim_sector ON CLUSTER cluster_2s2r
AS stock_data.dim_sector_local
ENGINE = Distributed(cluster_2s2r, stock_data, dim_sector_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.bridge_stock_index_local ON CLUSTER cluster_2s2r (
    stock_key       Int32,
    index_key       Int64,
    effective_from  Nullable(Date),
    effective_to    Nullable(Date),
    is_current      Nullable(Int8)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/bridge_stock_index',
    '{replica}'
)
ORDER BY (stock_key, index_key);

CREATE TABLE IF NOT EXISTS stock_data.bridge_stock_index ON CLUSTER cluster_2s2r
AS stock_data.bridge_stock_index_local
ENGINE = Distributed(cluster_2s2r, stock_data, bridge_stock_index_local, rand());


-- ╔═══════════════════════════════════════════════════════════════╗
-- ║                           FACTS                             ║
-- ╚═══════════════════════════════════════════════════════════════╝

CREATE TABLE IF NOT EXISTS stock_data.fact_market_equity_local ON CLUSTER cluster_2s2r (
    stock_key           Int32,
    date_key            Int32,
    `open`              Nullable(Float64),
    high                Nullable(Float64),
    low                 Nullable(Float64),
    `close`             Nullable(Float64),
    total_volume        Nullable(Int64),
    price_change_pct    Nullable(Float64),
    sma_20              Nullable(Float64),
    sma_50              Nullable(Float64),
    ema_12              Nullable(Float64),
    ema_26              Nullable(Float64),
    rsi_14              Nullable(Float64),
    macd                Nullable(Float64),
    vwap                Nullable(Float64),
    high_52w            Nullable(Float64),
    low_52w             Nullable(Float64),
    beta                Nullable(Float64),
    eps                 Nullable(Float64),
    bvps                Nullable(Float64),
    market_cap          Nullable(Float64),
    roe                 Nullable(Float64),
    dividend_yield      Nullable(Float64),
    pe                  Nullable(Float64),
    pb                  Nullable(Float64)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_market_equity',
    '{replica}'
)
ORDER BY (stock_key, date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_market_equity ON CLUSTER cluster_2s2r
AS stock_data.fact_market_equity_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_market_equity_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.fact_market_index_local ON CLUSTER cluster_2s2r (
    index_key           Int64,
    date_key            Int32,
    `open`              Nullable(Float64),
    high                Nullable(Float64),
    low                 Nullable(Float64),
    `close`             Nullable(Float64),
    total_volume        Nullable(Int64),
    price_change_pct    Nullable(Float64),
    sma_20              Nullable(Float64),
    sma_50              Nullable(Float64),
    ema_12              Nullable(Float64),
    ema_26              Nullable(Float64),
    rsi_14              Nullable(Float64),
    macd                Nullable(Float64)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_market_index',
    '{replica}'
)
ORDER BY (index_key, date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_market_index ON CLUSTER cluster_2s2r
AS stock_data.fact_market_index_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_market_index_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.fact_stock_signals_local ON CLUSTER cluster_2s2r (
    stock_key               Int32,
    date_key                Int32,
    signal_rsi              Nullable(String),
    signal_trend            Nullable(String),
    signal_macd             Nullable(String),
    signal_dividend         Nullable(String),
    signal_roe              Nullable(String),
    signal_pe               Nullable(String),
    signal_pb               Nullable(String),
    signal_price_pos        Nullable(String),
    label_stock_class       Nullable(String),
    label_trading_action    Nullable(String)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_stock_signals',
    '{replica}'
)
ORDER BY (stock_key, date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_stock_signals ON CLUSTER cluster_2s2r
AS stock_data.fact_stock_signals_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_stock_signals_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.fact_index_signals_local ON CLUSTER cluster_2s2r (
    index_key               Int64,
    date_key                Int32,
    signal_market_trend     Nullable(String),
    signal_market_rsi       Nullable(String)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_index_signals',
    '{replica}'
)
ORDER BY (index_key, date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_index_signals ON CLUSTER cluster_2s2r
AS stock_data.fact_index_signals_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_index_signals_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.fact_stock_events_local ON CLUSTER cluster_2s2r (
    stock_key           Int32,
    public_date_key     Int32,
    event_name_vi       Nullable(String),
    event_title_vi      Nullable(String),
    event_code          Nullable(String)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_stock_events',
    '{replica}'
)
ORDER BY (stock_key, public_date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_stock_events ON CLUSTER cluster_2s2r
AS stock_data.fact_stock_events_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_stock_events_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.fact_stock_news_local ON CLUSTER cluster_2s2r (
    stock_key           Int32,
    public_date_key     Int32,
    news_title          Nullable(String),
    news_short_content  Nullable(String),
    news_image_url      Nullable(String),
    news_source_link    Nullable(String)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_stock_news',
    '{replica}'
)
ORDER BY (stock_key, public_date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_stock_news ON CLUSTER cluster_2s2r
AS stock_data.fact_stock_news_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_stock_news_local, rand());


CREATE TABLE IF NOT EXISTS stock_data.fact_market_sector_local ON CLUSTER cluster_2s2r (
    sector_key          Int32,
    date_key            Int32,
    price_change_pct    Nullable(Float64),
    total_trade_value   Nullable(Float64),
    total_market_cap    Nullable(Float64),
    avg_pe              Nullable(Float64),
    avg_pb              Nullable(Float64),
    avg_eps             Nullable(Float64),
    stock_count         Nullable(Int32)
) ENGINE = ReplicatedReplacingMergeTree(
    '/clickhouse/tables/{shard}/stock_data/fact_market_sector',
    '{replica}'
)
ORDER BY (sector_key, date_key);

CREATE TABLE IF NOT EXISTS stock_data.fact_market_sector ON CLUSTER cluster_2s2r
AS stock_data.fact_market_sector_local
ENGINE = Distributed(cluster_2s2r, stock_data, fact_market_sector_local, rand());
