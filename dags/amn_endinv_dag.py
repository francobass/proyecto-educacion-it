"""DAG AMN EndInv: origen (amn) -> datalake (Parquet) -> bronze -> silver, un único día.

    extraer_a_datalake: SELECT del intervalo de la corrida en amn.endinv -> Parquet en
                        s3://datalake/source=amn/dataset=endinv/year=.../month=.../
    datalake_a_bronze:  lee ese Parquet y agrega sus filas a bronze.amn_endinv (append)
    bronze_a_silver:    limpia con SQL y hace upsert en silver.amn_endinv
                        y publica el asset silver.amn_endinv

El inventario es una foto al 2016-12-31 (enddate tiene un solo valor), así que el DAG
tiene una única corrida diaria: el intervalo [2016-12-31 00:00, día siguiente 00:00).
Antes de activarlo hay que cargar la base origen con los DAGs seed.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG, CronDataIntervalTimetable
from utils.assets import SILVER_ENDINV
from utils.layer import (
    CONN_DW,
    CREATED_BY,
    datalake_a_bronze,
    ejecutar_ddl,
    origen_a_datalake,
)


def extraer(**context) -> str:
    """Extrae con SQL el intervalo de la corrida y lo guarda como Parquet en el datalake."""
    return origen_a_datalake("endinv", columna_fecha="enddate", context=context)


def cargar_bronze(key: str, **context) -> int:
    """Crea las tablas si no existen y carga en bronze el Parquet que dejó la tarea anterior."""
    ejecutar_ddl("14_amn_endinv.sql")
    return datalake_a_bronze("amn_endinv", key=key, context=context)


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
            PARTITION BY TRIM(inventoryid)
            ORDER BY _created_at DESC
        ) AS rn
    FROM bronze.amn_endinv
    WHERE _created_by = %(created_by)s   -- las filas que cargó en bronze esta misma corrida
)
-- 2) Limpieza y upsert en silver
INSERT INTO silver.amn_endinv (
    inventory_id, store, city, brand, description, size, on_hand, price, end_date, _s3_path,
    _s3_version, _created_by, _created_at, _updated_by, _updated_at, _data_interval_start,
    _data_interval_end
)
SELECT
    TRIM(inventoryid),
    store::integer,
    NULLIF(TRIM(city), ''),                       -- la tienda 46 no tiene ciudad en el origen: queda NULL
    brand::integer,
    TRIM(description),
    NULLIF(TRIM(size), ''),
    onhand::integer,
    ROUND(price::numeric, 2),
    enddate::date,
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
  AND inventoryid IS NOT NULL
  AND onhand >= 0 AND price >= 0
-- Upsert: si la clave ya existe en silver, se actualiza en vez de duplicarse
ON CONFLICT (inventory_id) DO UPDATE SET
    store                = EXCLUDED.store,
    city                 = EXCLUDED.city,
    brand                = EXCLUDED.brand,
    description          = EXCLUDED.description,
    size                 = EXCLUDED.size,
    on_hand              = EXCLUDED.on_hand,
    price                = EXCLUDED.price,
    end_date             = EXCLUDED.end_date,
    _s3_path             = EXCLUDED._s3_path,
    _s3_version          = EXCLUDED._s3_version,
    -- _created_by y _created_at no se tocan: guardan quién creó la fila
    _updated_by          = EXCLUDED._updated_by,
    _updated_at          = EXCLUDED._updated_at,
    _data_interval_start = EXCLUDED._data_interval_start,
    _data_interval_end   = EXCLUDED._data_interval_end;
"""


with DAG(
    "amn_endinv",
    description="Inventario final AMN: origen -> datalake -> bronze (append) -> silver (upsert), un día",
    # Un único día: el inventario es una foto al 2016-12-31. start_date = end_date -> una sola corrida
    start_date=datetime(2016, 12, 31),
    end_date=datetime(2016, 12, 31),
    schedule=CronDataIntervalTimetable("@daily", timezone="UTC"),
    # Obligatorio: con catchup=False sólo se agenda el intervalo más reciente,
    # que cae después de end_date, y el DAG nunca corre.
    catchup=True,
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
            SILVER_ENDINV
        ],  # al terminar bien "actualiza" el asset: dispara el DAG gold_amn_purchases_model
    )

    extraer_task >> datalake_a_bronze_task >> bronze_a_silver_task
