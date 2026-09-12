CREATE SCHEMA IF NOT EXISTS staging;

CREATE TABLE IF NOT EXISTS staging.whale_alerts_limits (
    id BIGSERIAL PRIMARY KEY,
    coin_name VARCHAR(100) NOT NULL,
    known VARCHAR(50),
    unknown VARCHAR(50),
    extraction_ts TIMESTAMP NOT NULL
);