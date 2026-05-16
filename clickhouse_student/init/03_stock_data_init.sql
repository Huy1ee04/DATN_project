-- =============================================================
-- stock_data schema trên ClickHouse từ dữ liệu master ở MinIO
-- =============================================================
--
-- MinIO endpoint trong Docker network: http://minio:9000
-- Bucket mặc định: stock-data
-- Prefix nguồn: master/
--
-- Lưu ý:
-- - Dùng VIEW + table function s3(..., Parquet) để tự suy luận schema từ file parquet.
-- - Cách này phù hợp khi schema có thể thay đổi theo pipeline.

CREATE DATABASE IF NOT EXISTS stock_data;

/*
-- ===========================
-- Dimension
-- ===========================
CREATE OR REPLACE VIEW stock_data.dim_master_stock AS
SELECT *
FROM s3(
    'http://minio:9000/stock-data/master/dimension/dim_master_stock.parquet',
    'minio_access_key',
    'minio_secret_key',
    'Parquet'
);

CREATE OR REPLACE VIEW stock_data.dim_master_event AS
SELECT *
FROM s3(
    'http://minio:9000/stock-data/master/dimension/dim_master_event.parquet',
    'minio_access_key',
    'minio_secret_key',
    'Parquet'
);

-- ===========================
-- Fact (single file)
-- ===========================
CREATE OR REPLACE VIEW stock_data.fact_master_events AS
SELECT *
FROM s3(
    'http://minio:9000/stock-data/master/fact/fact_master_events.parquet',
    'minio_access_key',
    'minio_secret_key',
    'Parquet'
);

CREATE OR REPLACE VIEW stock_data.fact_master_news AS
SELECT *
FROM s3(
    'http://minio:9000/stock-data/master/fact/fact_master_news.parquet',
    'minio_access_key',
    'minio_secret_key',
    'Parquet'
);

-- ===========================
-- Fact market daily (partitioned)
-- ===========================
CREATE OR REPLACE VIEW stock_data.fact_market_equity_daily AS
SELECT *
FROM s3(
    'http://minio:9000/stock-data/master/fact/market/equity_daily/year=*/month=*/day=*/ohlc.parquet',
    'minio_access_key',
    'minio_secret_key',
    'Parquet'
);

CREATE OR REPLACE VIEW stock_data.fact_market_index_daily AS
SELECT *
FROM s3(
    'http://minio:9000/stock-data/master/fact/market/index_daily/year=*/month=*/day=*/ohlc.parquet',
    'minio_access_key',
    'minio_secret_key',
    'Parquet'
);
*/
