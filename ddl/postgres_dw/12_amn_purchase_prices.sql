-- AMN PurchasePrices: lista de precios de compra y venta por marca (snapshot, sin fecha).
-- Lo ejecuta postgres_db al crear el volumen y también el DAG amn_purchase_prices antes de cargar.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze: copia cruda (mismas columnas y tipos que el origen) + metadatos. Append puro:
-- si un día se re-ejecuta sus filas quedan repetidas (silver se queda con la más nueva).
CREATE TABLE IF NOT EXISTS bronze.amn_purchase_prices (
    brand           BIGINT,
    description     TEXT,
    price           DOUBLE PRECISION,
    size            TEXT,
    volume          TEXT,                   -- viene como texto: '750', 'Unknown', ...
    classification  BIGINT,
    purchaseprice   DOUBLE PRECISION,
    vendornumber    BIGINT,
    vendorname      TEXT,
    -- Metadatos (ver plugins/utils/layer.py)
    _s3_path             TEXT,                  -- Parquet del datalake del que salió la fila
    _s3_version          TEXT,                  -- VersionId de ese Parquet en MinIO
    _created_by          TEXT        NOT NULL,  -- "airflow__<dag_id>__<run_id>" que insertó la fila
    _created_at          TIMESTAMPTZ NOT NULL,
    _data_interval_start TIMESTAMPTZ NOT NULL,  -- intervalo de datos de la corrida de Airflow
    _data_interval_end   TIMESTAMPTZ NOT NULL
);
-- silver busca las filas de su corrida (_created_by)
CREATE INDEX IF NOT EXISTS ix_bronze_amn_purchase_prices_corrida ON bronze.amn_purchase_prices (_created_by);

-- Silver: un precio vigente por marca (upsert por brand).
CREATE TABLE IF NOT EXISTS silver.amn_purchase_prices (
    brand            INTEGER       NOT NULL,
    description      VARCHAR(100),
    size             VARCHAR(20),
    volume_ml        NUMERIC(10,2),         -- NULL si el origen dice 'Unknown'
    classification   SMALLINT,
    price            NUMERIC(12,2),         -- precio de venta
    purchase_price   NUMERIC(12,2),         -- precio de compra
    margin           NUMERIC(12,2),         -- price - purchase_price
    vendor_number    INTEGER,
    vendor_name      VARCHAR(100),
    -- Metadatos: linaje (de bronze) + auditoría (corrida de Airflow que creó / actualizó la fila)
    _s3_path              TEXT,
    _s3_version           TEXT,
    _created_by           TEXT          NOT NULL,
    _created_at           TIMESTAMPTZ   NOT NULL,
    _updated_by           TEXT          NOT NULL,
    _updated_at           TIMESTAMPTZ   NOT NULL,
    _data_interval_start  TIMESTAMPTZ   NOT NULL,
    _data_interval_end    TIMESTAMPTZ   NOT NULL,
    CONSTRAINT pk_silver_amn_purchase_prices PRIMARY KEY (brand)
);
