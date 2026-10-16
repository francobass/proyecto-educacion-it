"""Funciones para conectarse a MinIO y a PostgreSQL.

Los datos de conexión (host, usuario, contraseña) salen de variables de entorno definidas en
docker-compose.yaml.

- get_minio_client: la usan todos los DAGs para leer y guardar archivos en MinIO.
- crear_bucket_si_no_existe: crea un bucket de MinIO antes de escribir en él.
- get_postgres_engine: conexión directa a la base local_db, pensada para el DAG whale_alert.
  Los DAGs AMN y gold no la usan: se conectan con las Connections de Airflow amn_origen y
  postgres_dw (ver utils/layer.py).

Airflow agrega la carpeta plugins/ al path de Python, por eso desde un DAG se importa así:

    from utils.connections import get_minio_client, get_postgres_engine
"""

import os

import boto3
from botocore.exceptions import ClientError
from sqlalchemy import create_engine


def get_minio_client():
    """Devuelve un cliente S3 (boto3) apuntando a MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY", "admin"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY", "password"),
        region_name=os.getenv("MINIO_REGION", "us-east-1"),
    )


def crear_bucket_si_no_existe(client, bucket: str, versionado: bool = False) -> None:
    """Crea el bucket en MinIO sólo si todavía no existe.

    Con versionado=True cada vez que se sobrescribe un archivo se guarda una versión nueva
    con su propio VersionId (y las anteriores no se pierden).
    """
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError:
        try:
            client.create_bucket(Bucket=bucket)
        except ClientError as error:
            # Si otra corrida en paralelo lo creó un instante antes, no es un error
            if error.response["Error"]["Code"] not in (
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            ):
                raise
    if versionado:
        client.put_bucket_versioning(
            Bucket=bucket, VersioningConfiguration={"Status": "Enabled"}
        )


def get_postgres_engine():
    """Devuelve un engine de SQLAlchemy para la base de datos de los alumnos (local_db)."""
    return create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER', 'admin')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'admin123')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres_db')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'local_db')}"
    )
