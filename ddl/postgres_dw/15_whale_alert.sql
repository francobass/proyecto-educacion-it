-- Se ejecuta automáticamente la primera vez que se crea el volumen de postgres_db (base local_db).
-- Lo usan los DAGs whale_alert.

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE TABLE IF NOT EXISTS bronze.whale_alerts_limits (
    id BIGSERIAL PRIMARY KEY,
    coin_name VARCHAR(100) NOT NULL,
    known VARCHAR(50),
    -- unknown es palabra reservada de SQL, pero es el nombre de la columna en la página de Whale Alert
    unknown VARCHAR(50),  -- noqa: RF04
    extraction_ts TIMESTAMP NOT NULL
);
