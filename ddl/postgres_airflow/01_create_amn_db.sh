#!/bin/bash
# Crea el usuario y la base auxiliar "amn" (simula el sistema origen de AMN).
# Se ejecuta sólo la primera vez que se crea el volumen de postgres_airflow.
# Los valores vienen de AMN_DB_* en el .env.
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    CREATE USER "$AMN_DB_USER" WITH PASSWORD '$AMN_DB_PASSWORD';
    CREATE DATABASE "$AMN_DB_NAME" OWNER "$AMN_DB_USER";
EOSQL
