"""DAG AMN PurchasePrices: origen (amn) -> datalake (Parquet) -> bronze -> silver, snapshot completo.

    extraer_a_datalake: SELECT de toda amn.purchaseprices (no tiene fecha) -> Parquet en
                        s3://datalake/source=amn/dataset=purchaseprices/year=.../month=.../
    datalake_a_bronze:  lee ese Parquet y agrega sus filas a bronze.amn_purchase_prices (append)
    bronze_a_silver:    limpia con SQL y hace upsert en silver.amn_purchase_prices
                        y publica el asset silver.amn_purchase_prices

La lista de precios no tiene fecha, así que no se procesa por día: corre una sola vez (@once).
Si se vuelve a disparar a mano, carga un snapshot nuevo y silver se actualiza con el upsert.
Antes de activarlo hay que cargar la base origen con los DAGs seed.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from utils.assets import SILVER_PURCHASE_PRICES
from utils.layer import (
    CONN_DW,
    CREATED_BY,
    datalake_a_bronze,
    ejecutar_ddl,
    origen_a_datalake,
)


def extraer(**context) -> str:
    """Extrae con SQL la tabla completa (no tiene fecha) y lo guarda como Parquet en el datalake."""
    return origen_a_datalake("purchaseprices", columna_fecha=None, context=context)


def cargar_bronze(key: str, **context) -> int:
    """Crea las tablas si no existen y carga en bronze el Parquet que dejó la tarea anterior."""
    ejecutar_ddl("12_amn_purchase_prices.sql")
    return datalake_a_bronze("amn_purchase_prices", key=key, context=context)


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
            PARTITION BY brand
            ORDER BY _created_at DESC
        ) AS rn
    FROM bronze.amn_purchase_prices
    WHERE _created_by = %(created_by)s   -- las filas que cargó en bronze esta misma corrida
)
-- 2) Limpieza y upsert en silver
INSERT INTO silver.amn_purchase_prices (
    brand, description, size, volume_ml, classification, price, purchase_price, margin,
    vendor_number, vendor_name, _s3_path, _s3_version, _created_by, _created_at, _updated_by,
    _updated_at, _data_interval_start, _data_interval_end
)
SELECT
    brand::integer,
    TRIM(description),
    NULLIF(TRIM(size), ''),
    CASE WHEN TRIM(volume) ~ '^[0-9]+(\\.[0-9]+)?$' THEN TRIM(volume)::numeric END,-- '750' -> 750, 'Unknown' -> NULL
    classification::smallint,
    ROUND(price::numeric, 2),                     -- precio de venta
    ROUND(purchaseprice::numeric, 2),             -- precio de compra
    ROUND((price - purchaseprice)::numeric, 2),   -- margen bruto por unidad
    vendornumber::integer,
    INITCAP(TRIM(vendorname)),
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
  AND brand IS NOT NULL
  AND price >= 0 AND purchaseprice >= 0
-- Upsert: si la clave ya existe en silver, se actualiza en vez de duplicarse
ON CONFLICT (brand) DO UPDATE SET
    description          = EXCLUDED.description,
    size                 = EXCLUDED.size,
    volume_ml            = EXCLUDED.volume_ml,
    classification       = EXCLUDED.classification,
    price                = EXCLUDED.price,
    purchase_price       = EXCLUDED.purchase_price,
    margin               = EXCLUDED.margin,
    vendor_number        = EXCLUDED.vendor_number,
    vendor_name          = EXCLUDED.vendor_name,
    _s3_path             = EXCLUDED._s3_path,
    _s3_version          = EXCLUDED._s3_version,
    -- _created_by y _created_at no se tocan: guardan quién creó la fila
    _updated_by          = EXCLUDED._updated_by,
    _updated_at          = EXCLUDED._updated_at,
    _data_interval_start = EXCLUDED._data_interval_start,
    _data_interval_end   = EXCLUDED._data_interval_end;
"""


with DAG(
    "amn_purchase_prices",
    description="Precios AMN: origen -> datalake -> bronze (append) -> silver (upsert), snapshot",
    start_date=datetime(2016, 1, 1),
    schedule="@once",  # una sola corrida automática
    catchup=False,
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
            SILVER_PURCHASE_PRICES
        ],  # al terminar bien "actualiza" el asset: dispara el DAG gold_amn_purchases_model
    )

    extraer_task >> datalake_a_bronze_task >> bronze_a_silver_task
