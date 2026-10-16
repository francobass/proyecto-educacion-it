-- AMN VendorInvoices: facturas de proveedores (una fila por orden de compra).
-- Lo ejecuta postgres_db al crear el volumen y también el DAG amn_vendor_invoices antes de cargar.

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;

-- Bronze: copia cruda (mismas columnas y tipos que el origen) + metadatos. Append puro:
-- si un día se re-ejecuta sus filas quedan repetidas (silver se queda con la más nueva).
CREATE TABLE IF NOT EXISTS bronze.amn_vendor_invoices (
    vendornumber    BIGINT,
    vendorname      TEXT,
    invoicedate     TIMESTAMP,
    ponumber        BIGINT,
    podate          TIMESTAMP,
    paydate         TIMESTAMP,
    quantity        BIGINT,
    dollars         DOUBLE PRECISION,
    freight         DOUBLE PRECISION,
    approval        TEXT,
    -- Metadatos (ver plugins/utils/layer.py)
    _s3_path             TEXT,                  -- Parquet del datalake del que salió la fila
    _s3_version          TEXT,                  -- VersionId de ese Parquet en MinIO
    _created_by          TEXT        NOT NULL,  -- "airflow__<dag_id>__<run_id>" que insertó la fila
    _created_at          TIMESTAMPTZ NOT NULL,
    _data_interval_start TIMESTAMPTZ NOT NULL,  -- intervalo de datos de la corrida de Airflow
    _data_interval_end   TIMESTAMPTZ NOT NULL
);
-- silver busca las filas de su corrida (_created_by)
CREATE INDEX IF NOT EXISTS ix_bronze_amn_vendor_invoices_corrida ON bronze.amn_vendor_invoices (_created_by);

-- Silver: datos limpios y tipados (upsert por número de orden).
CREATE TABLE IF NOT EXISTS silver.amn_vendor_invoices (
    po_number        BIGINT        NOT NULL,
    vendor_number    INTEGER,
    vendor_name      VARCHAR(100),
    invoice_date     DATE,
    po_date          DATE,
    pay_date         DATE,
    quantity         INTEGER,
    dollars          NUMERIC(14,2),
    freight          NUMERIC(12,2),
    approved_by      VARCHAR(50),           -- NULL si la factura no requirió aprobación
    is_approved      BOOLEAN       NOT NULL,
    -- Metadatos: linaje (de bronze) + auditoría (corrida de Airflow que creó / actualizó la fila)
    _s3_path              TEXT,
    _s3_version           TEXT,
    _created_by           TEXT          NOT NULL,
    _created_at           TIMESTAMPTZ   NOT NULL,
    _updated_by           TEXT          NOT NULL,
    _updated_at           TIMESTAMPTZ   NOT NULL,
    _data_interval_start  TIMESTAMPTZ   NOT NULL,
    _data_interval_end    TIMESTAMPTZ   NOT NULL,
    CONSTRAINT pk_silver_amn_vendor_invoices PRIMARY KEY (po_number)
);
