-- AMN BegInv: inventario inicial (al 2016-01-01), una fila por producto y tienda.
-- Lo ejecuta postgres_db al crear el volumen y también el DAG amn_beginv antes de cargar.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze: copia cruda (mismas columnas y tipos que el origen) + metadatos. Append puro:
-- si un día se re-ejecuta sus filas quedan repetidas (silver se queda con la más nueva).
CREATE TABLE IF NOT EXISTS bronze.amn_beginv (
    inventoryid     TEXT,
    store           BIGINT,
    city            TEXT,
    brand           BIGINT,
    description     TEXT,
    size            TEXT,
    onhand          BIGINT,
    price           DOUBLE PRECISION,
    startdate       TIMESTAMP,
    -- Metadatos (ver plugins/utils/layer.py)
    _s3_path             TEXT,                  -- Parquet del datalake del que salió la fila
    _s3_version          TEXT,                  -- VersionId de ese Parquet en MinIO
    _created_by          TEXT        NOT NULL,  -- "airflow__<dag_id>__<run_id>" que insertó la fila
    _created_at          TIMESTAMPTZ NOT NULL,
    _data_interval_start TIMESTAMPTZ NOT NULL,  -- intervalo de datos de la corrida de Airflow
    _data_interval_end   TIMESTAMPTZ NOT NULL
);
-- silver busca las filas de su corrida (_created_by)
CREATE INDEX IF NOT EXISTS ix_bronze_amn_beginv_corrida ON bronze.amn_beginv (_created_by);

-- Silver: datos limpios y tipados (upsert por inventory_id = tienda + ciudad + marca).
CREATE TABLE IF NOT EXISTS silver.amn_beginv (
    inventory_id     VARCHAR(30)   NOT NULL,
    store            INTEGER,
    city             VARCHAR(50),
    brand            INTEGER,
    description      VARCHAR(100),
    size             VARCHAR(20),
    on_hand          INTEGER,
    price            NUMERIC(12,2),
    start_date       DATE,
    -- Metadatos: linaje (de bronze) + auditoría (corrida de Airflow que creó / actualizó la fila)
    _s3_path              TEXT,
    _s3_version           TEXT,
    _created_by           TEXT          NOT NULL,
    _created_at           TIMESTAMPTZ   NOT NULL,
    _updated_by           TEXT          NOT NULL,
    _updated_at           TIMESTAMPTZ   NOT NULL,
    _data_interval_start  TIMESTAMPTZ   NOT NULL,
    _data_interval_end    TIMESTAMPTZ   NOT NULL,
    CONSTRAINT pk_silver_amn_beginv PRIMARY KEY (inventory_id)
);
