"""DAG seed de la dimensión tiempo: carga gold.dim_tiempo con un día por fila (2015 a 2027).

    crear_tablas      -> ejecuta ddl/postgres_dw/20_gold.sql (CREATE ... IF NOT EXISTS)
    cargar_dim_tiempo -> genera los días con SQL (generate_series) y hace upsert

Corre una sola vez (@once). Al terminar publica el asset gold.dim_tiempo, que dispara gold_amn_purchases_model.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from utils.assets import GOLD_DIM_TIEMPO
from utils.layer import CONN_DW, CREATED_BY, ejecutar_ddl

# Rango de fechas: cubre todas las fechas de los datos de AMN (2015-12 a 2017-02).
# Si falta un día, fact_compras falla por la FK a dim_tiempo.
FECHA_DESDE = "2015-01-01"
FECHA_HASTA = "2027-12-31"

CARGAR_DIM_TIEMPO = """
INSERT INTO gold.dim_tiempo (
    fecha_key, fecha, anio, trimestre, mes, nombre_mes, semana_iso,
    dia, dia_semana, nombre_dia, es_fin_de_semana,
    _created_by, _created_at, _updated_by, _updated_at
)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::integer,          -- 2016-03-15 -> 20160315
    d::date,
    EXTRACT(YEAR FROM d),
    EXTRACT(QUARTER FROM d),
    EXTRACT(MONTH FROM d),
    -- Nombres en español sin depender del idioma del servidor
    (ARRAY['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio',
           'Agosto','Septiembre','Octubre','Noviembre','Diciembre'])[EXTRACT(MONTH FROM d)::int],
    EXTRACT(WEEK FROM d),                      -- semana ISO: 2016-01-01 es la semana 53 de 2015 (no combinar con anio)
    EXTRACT(DAY FROM d),
    EXTRACT(ISODOW FROM d),                    -- 1 = lunes ... 7 = domingo
    (ARRAY['Lunes','Martes','Miércoles','Jueves','Viernes','Sábado','Domingo'])[EXTRACT(ISODOW FROM d)::int],
    EXTRACT(ISODOW FROM d) IN (6, 7),
    %(created_by)s, now(), %(created_by)s, now()      -- auditoría: corrida de Airflow
-- generate_series crea una fila por día entre las dos fechas
FROM generate_series(%(desde)s::date, %(hasta)s::date, interval '1 day') AS d
-- Upsert: se puede volver a correr sin duplicar días
ON CONFLICT (fecha_key) DO UPDATE SET
    anio             = EXCLUDED.anio,
    trimestre        = EXCLUDED.trimestre,
    mes              = EXCLUDED.mes,
    nombre_mes       = EXCLUDED.nombre_mes,
    semana_iso       = EXCLUDED.semana_iso,
    dia              = EXCLUDED.dia,
    dia_semana       = EXCLUDED.dia_semana,
    nombre_dia       = EXCLUDED.nombre_dia,
    es_fin_de_semana = EXCLUDED.es_fin_de_semana,
    _updated_by      = EXCLUDED._updated_by,
    _updated_at      = EXCLUDED._updated_at;
"""

with DAG(
    "gold_seed_dim_tiempo",
    description="Carga la dimensión tiempo (gold.dim_tiempo)",
    schedule="@once",  # una sola corrida automática
    start_date=datetime(2023, 1, 1),
    catchup=False,
):
    crear_tablas = PythonOperator(
        task_id="crear_tablas",
        python_callable=ejecutar_ddl,
        op_kwargs={"archivo": "20_gold.sql"},
    )

    cargar_dim_tiempo = SQLExecuteQueryOperator(
        task_id="cargar_dim_tiempo",
        conn_id=CONN_DW,
        sql=CARGAR_DIM_TIEMPO,
        parameters={
            "desde": FECHA_DESDE,
            "hasta": FECHA_HASTA,
            "created_by": CREATED_BY,
        },
        outlets=[GOLD_DIM_TIEMPO],  # dispara gold_amn_purchases_model
    )

    crear_tablas >> cargar_dim_tiempo
