-- Gold: modelo estrella de compras de AMN (capa de negocio, nombres en español).
-- Lo ejecuta postgres_db al crear el volumen y también los DAGs gold_seed_dim_tiempo y gold_amn_purchases_model.
--
--                    dim_tiempo (x2: fecha de orden y de recepción)
--                         |
--   dim_producto ---- fact_compras ---- dim_proveedor
--                         |
--                    dim_tienda

CREATE SCHEMA IF NOT EXISTS gold;

-- Dimensión tiempo: un día por fila. La carga el DAG gold_seed_dim_tiempo.
CREATE TABLE IF NOT EXISTS gold.dim_tiempo (
    fecha_key         INTEGER      NOT NULL,      -- formato AAAAMMDD, ej. 20160315
    fecha             DATE         NOT NULL UNIQUE,
    anio              SMALLINT     NOT NULL,
    trimestre         SMALLINT     NOT NULL,
    mes               SMALLINT     NOT NULL,
    nombre_mes        VARCHAR(10)  NOT NULL,
    semana_iso        SMALLINT     NOT NULL,
    dia               SMALLINT     NOT NULL,
    dia_semana        SMALLINT     NOT NULL,      -- 1 = lunes ... 7 = domingo
    nombre_dia        VARCHAR(10)  NOT NULL,
    es_fin_de_semana  BOOLEAN      NOT NULL,
    -- Auditoría: corrida de Airflow ("airflow__<dag_id>__<run_id>") que creó / actualizó la fila
    _created_by       TEXT           NOT NULL,
    _created_at       TIMESTAMPTZ    NOT NULL,
    _updated_by       TEXT           NOT NULL,
    _updated_at       TIMESTAMPTZ    NOT NULL,
    CONSTRAINT pk_gold_dim_tiempo PRIMARY KEY (fecha_key)
);

-- Dimensión producto: una fila por marca (brand)
CREATE TABLE IF NOT EXISTS gold.dim_producto (
    producto_key     INTEGER        NOT NULL,     -- brand
    descripcion      VARCHAR(100),
    tamanio          VARCHAR(20),
    volumen_ml       NUMERIC(10,2),
    clasificacion    SMALLINT,
    precio_venta     NUMERIC(12,2),               -- NULL si la marca no está en la lista de precios
    precio_compra    NUMERIC(12,2),
    margen_unitario  NUMERIC(12,2),
    -- Auditoría: corrida de Airflow ("airflow__<dag_id>__<run_id>") que creó / actualizó la fila
    _created_by       TEXT           NOT NULL,
    _created_at       TIMESTAMPTZ    NOT NULL,
    _updated_by       TEXT           NOT NULL,
    _updated_at       TIMESTAMPTZ    NOT NULL,
    CONSTRAINT pk_gold_dim_producto PRIMARY KEY (producto_key)
);

-- Dimensión proveedor
CREATE TABLE IF NOT EXISTS gold.dim_proveedor (
    proveedor_key    INTEGER        NOT NULL,     -- vendor_number
    nombre           VARCHAR(100),
    -- Auditoría: corrida de Airflow ("airflow__<dag_id>__<run_id>") que creó / actualizó la fila
    _created_by       TEXT           NOT NULL,
    _created_at       TIMESTAMPTZ    NOT NULL,
    _updated_by       TEXT           NOT NULL,
    _updated_at       TIMESTAMPTZ    NOT NULL,
    CONSTRAINT pk_gold_dim_proveedor PRIMARY KEY (proveedor_key)
);

-- Dimensión tienda
CREATE TABLE IF NOT EXISTS gold.dim_tienda (
    tienda_key       INTEGER        NOT NULL,     -- store
    ciudad           VARCHAR(50),                 -- NULL si el origen no la informa (tienda 46)
    -- Auditoría: corrida de Airflow ("airflow__<dag_id>__<run_id>") que creó / actualizó la fila
    _created_by       TEXT           NOT NULL,
    _created_at       TIMESTAMPTZ    NOT NULL,
    _updated_by       TEXT           NOT NULL,
    _updated_at       TIMESTAMPTZ    NOT NULL,
    CONSTRAINT pk_gold_dim_tienda PRIMARY KEY (tienda_key)
);

-- Hechos: una fila por línea de compra recibida (misma clave que silver.amn_purchases).
CREATE TABLE IF NOT EXISTS gold.fact_compras (
    fecha_orden_key         INTEGER        NOT NULL REFERENCES gold.dim_tiempo (fecha_key),
    numero_orden            BIGINT         NOT NULL,
    inventory_id            VARCHAR(30)    NOT NULL,
    fecha_recepcion_key     INTEGER        NOT NULL REFERENCES gold.dim_tiempo (fecha_key),
    producto_key            INTEGER        REFERENCES gold.dim_producto (producto_key),
    proveedor_key           INTEGER        REFERENCES gold.dim_proveedor (proveedor_key),
    tienda_key              INTEGER        REFERENCES gold.dim_tienda (tienda_key),
    -- Medidas
    cantidad                INTEGER,
    precio_compra_unitario  NUMERIC(12,2),
    importe_compra          NUMERIC(14,2),
    precio_venta_unitario   NUMERIC(12,2),
    -- Estimado: cantidad * (precio de venta - precio de compra), con el precio de venta de la lista de dic-2017 aplicado a compras de 2016
    margen_estimado         NUMERIC(14,2),
    -- Auditoría: corrida de Airflow ("airflow__<dag_id>__<run_id>") que creó / actualizó la fila
    _created_by       TEXT           NOT NULL,
    _created_at       TIMESTAMPTZ    NOT NULL,
    _updated_by       TEXT           NOT NULL,
    _updated_at       TIMESTAMPTZ    NOT NULL,
    CONSTRAINT pk_gold_fact_compras PRIMARY KEY (numero_orden, inventory_id, fecha_recepcion_key)
);
-- Índices para cruzar con las dimensiones más usadas
CREATE INDEX IF NOT EXISTS ix_fact_compras_producto ON gold.fact_compras (producto_key);
CREATE INDEX IF NOT EXISTS ix_fact_compras_fecha_orden ON gold.fact_compras (fecha_orden_key);
