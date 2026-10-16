-- AMN Purchases: compras a proveedores (una fila por recepción de un producto de una orden de compra).
-- Lo ejecuta postgres_db al crear el volumen y también el DAG amn_purchases antes de cargar.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze: copia cruda (mismas columnas y tipos que el origen) + metadatos. Append puro:
-- si un día se re-ejecuta sus filas quedan repetidas (silver se queda con la más nueva).
CREATE TABLE IF NOT EXISTS bronze.amn_purchases (
    inventoryid     TEXT,
    store           BIGINT,
    brand           BIGINT,
    description     TEXT,
    size            TEXT,
    vendornumber    BIGINT,
    vendorname      TEXT,
    ponumber        BIGINT,
    podate          TIMESTAMP,
    receivingdate   TIMESTAMP,
    invoicedate     TIMESTAMP,
    paydate         TIMESTAMP,
    purchaseprice   DOUBLE PRECISION,
    quantity        BIGINT,
    dollars         DOUBLE PRECISION,
    classification  BIGINT,
    -- Metadatos (ver plugins/utils/layer.py)
    _s3_path             TEXT,                  -- Parquet del datalake del que salió la fila
    _s3_version          TEXT,                  -- VersionId de ese Parquet en MinIO
    _created_by          TEXT        NOT NULL,  -- "airflow__<dag_id>__<run_id>" que insertó la fila
    _created_at          TIMESTAMPTZ NOT NULL,
    _data_interval_start TIMESTAMPTZ NOT NULL,  -- intervalo de datos de la corrida de Airflow
    _data_interval_end   TIMESTAMPTZ NOT NULL
);
-- silver busca las filas de su corrida (_created_by)
CREATE INDEX IF NOT EXISTS ix_bronze_amn_purchases_corrida ON bronze.amn_purchases (_created_by);

-- Silver: datos limpios, tipados y sin duplicados. Se actualiza con upsert.
CREATE TABLE IF NOT EXISTS silver.amn_purchases (
    po_date          DATE          NOT NULL,
    po_number        BIGINT        NOT NULL,
    inventory_id     VARCHAR(30)   NOT NULL,
    receiving_date   DATE          NOT NULL,
    store            INTEGER,
    brand            INTEGER,
    description      VARCHAR(100),
    size             VARCHAR(20),
    vendor_number    INTEGER,
    vendor_name      VARCHAR(100),
    invoice_date     DATE,
    pay_date         DATE,
    purchase_price   NUMERIC(12,2),
    quantity         INTEGER,
    dollars          NUMERIC(14,2),
    classification   SMALLINT,
    -- Metadatos: linaje (de bronze) + auditoría (corrida de Airflow que creó / actualizó la fila)
    _s3_path              TEXT,
    _s3_version           TEXT,
    _created_by           TEXT          NOT NULL,
    _created_at           TIMESTAMPTZ   NOT NULL,
    _updated_by           TEXT          NOT NULL,
    _updated_at           TIMESTAMPTZ   NOT NULL,
    _data_interval_start  TIMESTAMPTZ   NOT NULL,
    _data_interval_end    TIMESTAMPTZ   NOT NULL,
    -- Una orden puede recibir el mismo producto en varias fechas (entregas parciales)
    CONSTRAINT pk_silver_amn_purchases PRIMARY KEY (po_number, inventory_id, receiving_date)
);
