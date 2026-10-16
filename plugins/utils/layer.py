"""Funciones comunes de los DAGs AMN: origen (base amn) -> datalake (MinIO, Parquet) -> bronze.

Usan las conexiones de Airflow (Admin > Connections), creadas por airflow-init:
    amn_origen  -> base amn en postgres_airflow (sistema origen)
    postgres_dw -> base local_db en postgres_db (data warehouse)

Columnas de metadatos que agrega bronze a cada fila:
    _s3_path, _s3_version  -> archivo Parquet del datalake (y su VersionId) del que salió la fila
    _created_by            -> "airflow__<dag_id>__<run_id>": la corrida de Airflow que insertó la fila
    _created_at            -> momento en que se insertó
    _data_interval_start   -> intervalo de datos de esa corrida (para los DAGs diarios: un día)
    _data_interval_end
"""

import io
import logging
from pathlib import Path

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from sqlalchemy import text
from utils.connections import crear_bucket_si_no_existe, get_minio_client

log = logging.getLogger(__name__)

CONN_ORIGEN = "amn_origen"
CONN_DW = "postgres_dw"

# Bucket del data lake (el mismo que usa whale alert). Tiene versionado: cada archivo
# sobrescrito guarda una versión nueva con su VersionId.
BUCKET_DATALAKE = "datalake"
SOURCE = "amn"

# Carpeta ddl/postgres_dw montada en los contenedores de Airflow (ver docker-compose.yaml)
DDL_DIR = Path("/opt/airflow/ddl/postgres_dw")

# Valor de _created_by / _updated_by para usar como parámetro en los SQL (Airflow lo completa)
CREATED_BY = "airflow__{{ dag.dag_id }}__{{ run_id }}"


def ejecutar_ddl(archivo: str) -> None:
    """Ejecuta un archivo de ddl/postgres_dw (CREATE ... IF NOT EXISTS) en el data warehouse."""
    sql = (DDL_DIR / archivo).read_text(encoding="utf-8")
    PostgresHook(postgres_conn_id=CONN_DW).run(sql)


def _intervalo(context: dict):
    """Devuelve (desde, hasta) de la corrida. Sin intervalo (disparo manual) usa el momento de la corrida.

    Ojo: un disparo manual cae en un intervalo actual (sin datos de 2016) o vacío (desde == hasta)
    y extrae 0 filas. Para reprocesar un día, hacer Clear sobre su corrida o ejecutar backfill.
    """
    dag_run = context["dag_run"]
    desde = context.get("data_interval_start") or dag_run.run_after
    hasta = context.get("data_interval_end") or dag_run.run_after
    return desde, hasta


def origen_a_datalake(
    tabla_origen: str, columna_fecha: str | None, context: dict
) -> str:
    """Extrae con SQL las filas del intervalo de la corrida y las guarda como Parquet en el datalake.

    Filtra columna_fecha >= data_interval_start y < data_interval_end.
    Si columna_fecha es None extrae la tabla completa (snapshot). Devuelve la key del archivo
    (queda en XCom para la tarea siguiente).
    """
    desde, hasta = _intervalo(context)

    consulta = f"SELECT * FROM {tabla_origen}"
    if columna_fecha:
        consulta += f" WHERE {columna_fecha} >= :desde AND {columna_fecha} < :hasta"

    # Las fechas del origen no tienen zona horaria: al intervalo (en UTC) solo le sacamos la zona, sin convertir.
    # Es correcto porque los DAGs usan timezone="UTC"; con otra zona el día quedaría corrido.
    params = {"desde": desde.replace(tzinfo=None), "hasta": hasta.replace(tzinfo=None)}
    # En el log va la consulta con los valores ya puestos, para poder copiarla y correrla a mano
    consulta_log = consulta.replace(":desde", f"'{params['desde']}'").replace(
        ":hasta", f"'{params['hasta']}'"
    )
    log.info("SQL al origen (%s):\n%s", CONN_ORIGEN, consulta_log)

    origen = PostgresHook(postgres_conn_id=CONN_ORIGEN).get_sqlalchemy_engine()
    with origen.connect() as conn:
        df = pd.read_sql(text(consulta), conn, params=params)
    origen.dispose()
    log.info(
        "Origen %s, intervalo [%s, %s): %d filas", tabla_origen, desde, hasta, len(df)
    )

    # Ruta particionada por fecha, igual que en whale alert:
    # source=amn/dataset=purchases/year=2016/month=03/20160301T000000_purchases.parquet
    key = (
        f"source={SOURCE}/dataset={tabla_origen}/year={desde:%Y}/month={desde:%m}/"
        f"{desde:%Y%m%dT%H%M%S}_{tabla_origen}.parquet"
    )

    # Se escribe el archivo aunque no haya filas: así la tarea siguiente siempre lo encuentra.
    # Sin filas, pandas no conoce los tipos: el Parquet guarda solo las columnas (bronze lo saltea por df.empty).
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)

    minio = get_minio_client()
    crear_bucket_si_no_existe(minio, BUCKET_DATALAKE, versionado=True)
    respuesta = minio.put_object(
        Bucket=BUCKET_DATALAKE, Key=key, Body=buffer.getvalue()
    )
    log.info(
        "Parquet subido: s3://%s/%s (versión %s)",
        BUCKET_DATALAKE,
        key,
        respuesta.get("VersionId"),
    )
    return key


def datalake_a_bronze(tabla_bronze: str, key: str, context: dict) -> int:
    """Lee el Parquet `key` del datalake y agrega sus filas a bronze.<tabla_bronze> (append).

    Devuelve la cantidad de filas cargadas.
    """
    minio = get_minio_client()
    obj = minio.get_object(Bucket=BUCKET_DATALAKE, Key=key)
    df = pd.read_parquet(io.BytesIO(obj["Body"].read()))
    log.info("Leído s3://%s/%s: %d filas", BUCKET_DATALAKE, key, len(df))

    if df.empty:
        return 0  # un día sin datos no es un error

    desde, hasta = _intervalo(context)
    # Metadatos: de qué archivo viene la fila y qué corrida de Airflow la cargó
    df["_s3_path"] = f"s3://{BUCKET_DATALAKE}/{key}"
    df["_s3_version"] = obj.get("VersionId")
    # Mismo texto que CREATED_BY: bronze_a_silver busca por este valor las filas de su corrida
    df["_created_by"] = f"airflow__{context['dag_run'].dag_id}__{context['run_id']}"
    df["_created_at"] = pd.Timestamp.now(tz="UTC")
    df["_data_interval_start"] = pd.Timestamp(desde)
    df["_data_interval_end"] = pd.Timestamp(hasta)

    dw = PostgresHook(postgres_conn_id=CONN_DW).get_sqlalchemy_engine()
    df.to_sql(
        tabla_bronze,
        dw,
        schema="bronze",
        if_exists="append",
        index=False,
        chunksize=10_000,
    )
    dw.dispose()
    log.info("Cargadas %d filas en bronze.%s", len(df), tabla_bronze)
    return len(df)
