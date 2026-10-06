-- Se ejecuta automáticamente la primera vez que se crea el volumen de postgres_db (base local_db).
-- Lo usan los DAGs whale_alert.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.whale_alerts_limits (
    id BIGSERIAL PRIMARY KEY,
    coin_name VARCHAR(100) NOT NULL,
    known VARCHAR(50),
    unknown VARCHAR(50),
    extraction_ts TIMESTAMP NOT NULL
);
