"""DAG AMN Purchases: origen (amn) -> datalake (Parquet) -> bronze -> silver, un día por corrida.

    extraer_a_datalake: SELECT del intervalo de la corrida en amn.purchases -> Parquet en
                        s3://datalake/source=amn/dataset=purchases/year=.../month=.../
    datalake_a_bronze:  lee ese Parquet y agrega sus filas a bronze.amn_purchases (append)
    bronze_a_silver:    limpia ese día con SQL y hace upsert en silver.amn_purchases
                        y publica el asset silver.amn_purchases

Backfill: catchup=True crea una corrida por cada día de marzo y abril de 2016 (61 corridas).
Los sábados no hay compras: esas corridas cargan 0 filas sin fallar.
Antes de activarlo hay que cargar la base origen con los DAGs seed (disparar aux_amn_0_master_ingesta).
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, CronDataIntervalTimetable
from utils.assets import SILVER_PURCHASES
from utils.layer import (
    CONN_DW,
    CREATED_BY,
    datalake_a_bronze,
    ejecutar_ddl,
    origen_a_datalake,
)


def extraer(**context) -> str:
    """Extrae con SQL el intervalo de la corrida y lo guarda como Parquet en el datalake."""
    return origen_a_datalake("purchases", columna_fecha="podate", context=context)


def cargar_bronze(key: str, **context) -> int:
    """Crea las tablas si no existen y carga en bronze el Parquet que dejó la tarea anterior."""
    ejecutar_ddl("10_amn_purchases.sql")
    return datalake_a_bronze("amn_purchases", key=key, context=context)


# Limpieza bronze -> silver. Airflow renderiza CREATED_BY ("airflow__<dag_id>__<run_id>") porque
# parameters acepta plantillas; después psycopg2 pone ese valor en %(created_by)s.
UPSERT_SILVER = """
-- 1) ultimas: las filas que cargó esta corrida en bronze, numeradas por clave de silver.
--    ROW_NUMBER() = 1 es la carga más nueva de cada clave (si el día se cargó más de una vez).
--    Es imprescindible: con una clave repetida, ON CONFLICT falla con "cannot affect row a second time".
WITH ultimas AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY ponumber, TRIM(inventoryid), receivingdate::date
            ORDER BY _created_at DESC
        ) AS rn
    FROM bronze.amn_purchases
    WHERE _created_by = %(created_by)s   -- las filas que cargó en bronze esta misma corrida
)
-- 2) Limpieza y upsert en silver
INSERT INTO silver.amn_purchases (
    po_date, po_number, inventory_id, receiving_date, store, brand, description, size,
    vendor_number, vendor_name, invoice_date, pay_date, purchase_price, quantity, dollars,
    classification, _s3_path, _s3_version, _created_by, _created_at, _updated_by, _updated_at,
    _data_interval_start, _data_interval_end
)
SELECT
    podate::date,
    ponumber,
    TRIM(inventoryid),
    receivingdate::date,
    store::integer,
    brand::integer,
    TRIM(description),
    NULLIF(TRIM(size), ''),                       -- texto vacío -> NULL
    vendornumber::integer,
    INITCAP(TRIM(vendorname)),                    -- 'ALTAMAR BRANDS LLC   ' -> 'Altamar Brands Llc'
    invoicedate::date,
    paydate::date,
    ROUND(purchaseprice::numeric, 2),
    quantity::integer,
    ROUND(dollars::numeric, 2),
    classification::smallint,
    _s3_path,
    _s3_version,
    %(created_by)s, now(),                       -- _created_by, _created_at (sólo cuentan al insertar)
    %(created_by)s, now(),                       -- _updated_by, _updated_at
    _data_interval_start,
    _data_interval_end
FROM ultimas
WHERE rn = 1
  -- Descartamos filas sin clave o con valores imposibles
  -- (NULL >= 0 no es verdadero: una medida NULL también descarta la fila)
  AND podate IS NOT NULL AND ponumber IS NOT NULL AND inventoryid IS NOT NULL AND receivingdate IS NOT NULL
  AND quantity >= 0 AND dollars >= 0
-- Upsert: si la clave ya existe en silver, se actualiza en vez de duplicarse
ON CONFLICT (po_number, inventory_id, receiving_date) DO UPDATE SET
    po_date              = EXCLUDED.po_date,
    store                = EXCLUDED.store,
    brand                = EXCLUDED.brand,
    description          = EXCLUDED.description,
    size                 = EXCLUDED.size,
    vendor_number        = EXCLUDED.vendor_number,
    vendor_name          = EXCLUDED.vendor_name,
    invoice_date         = EXCLUDED.invoice_date,
    pay_date             = EXCLUDED.pay_date,
    purchase_price       = EXCLUDED.purchase_price,
    quantity             = EXCLUDED.quantity,
    dollars              = EXCLUDED.dollars,
    classification       = EXCLUDED.classification,
    _s3_path             = EXCLUDED._s3_path,
    _s3_version          = EXCLUDED._s3_version,
    -- _created_by y _created_at no se tocan: guardan quién creó la fila
    _updated_by          = EXCLUDED._updated_by,
    _updated_at          = EXCLUDED._updated_at,
    _data_interval_start = EXCLUDED._data_interval_start,
    _data_interval_end   = EXCLUDED._data_interval_end;
"""


with DAG(
    "amn_purchases",
    description="Compras AMN: origen -> datalake -> bronze (append) -> silver (upsert), por día",
    # Procesamos sólo dos meses de ejemplo para que el backfill sea corto
    start_date=datetime(2016, 3, 1),
    end_date=datetime(2016, 4, 30),
    # Intervalo de un día: la corrida del 2016-03-01 procesa [03-01 00:00, 03-02 00:00)
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    catchup=True,  # crea una corrida por cada día del rango (backfill)
    max_active_runs=3,  # hasta 3 días procesándose a la vez
):
    extraer_task = PythonOperator(
        task_id="extraer_a_datalake",
        python_callable=extraer,
    )

    datalake_a_bronze_task = PythonOperator(
        task_id="datalake_a_bronze",
        python_callable=cargar_bronze,
        # La key del Parquet la devolvió extraer_a_datalake (queda en XCom)
        op_kwargs={"key": "{{ ti.xcom_pull(task_ids='extraer_a_datalake') }}"},
    )

    bronze_a_silver_task = SQLExecuteQueryOperator(
        task_id="bronze_a_silver",
        conn_id=CONN_DW,
        sql=UPSERT_SILVER,
        parameters={"created_by": CREATED_BY},
        outlets=[
            SILVER_PURCHASES
        ],  # al terminar bien "actualiza" el asset: dispara el DAG gold_amn_purchases_model
    )

    extraer_task >> datalake_a_bronze_task >> bronze_a_silver_task
