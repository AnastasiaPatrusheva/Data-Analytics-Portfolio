-- Витрины ETL-пайплайна стримингового сервиса (ClickHouse, MergeTree).
-- Партиционирование по бизнес-дате: загрузка идемпотентна на уровне партиции
-- (DROP PARTITION + INSERT) — это нужно для backfill и повторных запусков DAG.

CREATE TABLE IF NOT EXISTS mart_daily_content_type
(
    business_dt        Date,
    main_content_type  LowCardinality(String),
    adult_content_flg  UInt8,
    kids_content_flg   UInt8,
    is_weekend         UInt8,
    total_minutes      Int64,
    sessions           Int64,
    unique_users       Int64
)
ENGINE = MergeTree
PARTITION BY business_dt
ORDER BY (business_dt, main_content_type);

CREATE TABLE IF NOT EXISTS mart_geo
(
    business_dt         Date,
    usage_country_name  LowCardinality(String),
    usage_geo_id_name   String,
    total_minutes       Int64,
    sessions            Int64,
    unique_users        Int64
)
ENGINE = MergeTree
PARTITION BY business_dt
ORDER BY (business_dt, usage_country_name, usage_geo_id_name);

-- Агрегаты по каждому произведению за день; топ-N строится запросом (ORDER BY … LIMIT).
CREATE TABLE IF NOT EXISTS mart_content_top
(
    business_dt        Date,
    main_content_id    String,
    main_content_name  String,
    main_content_type  LowCardinality(String),
    total_minutes      Int64,
    sessions           Int64,
    unique_users       Int64
)
ENGINE = MergeTree
PARTITION BY business_dt
ORDER BY (business_dt, main_content_id);
