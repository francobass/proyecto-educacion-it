# proyecto-educacion-it

Repositorio para el proyecto integrador del curso de ingeniería de datos de Educación IT.

## Prerrequisitos

- Git
- Docker
- Python


## Git

Clonar el repositorio:

```bash
git clone https://github.com/francobass/proyecto-educacion-it
```

Comandos habituales:

```bash
git checkout main                          # cambiar a rama main
git pull                                   # traer lo último de main
git checkout -b feature/whale-alert main   # crear una branch clonada desde main
git status                                 # revisar qué se va a subir
git add .                                  # stagear todos los cambios
git commit -m "Mensaje del commit"         # hacer commit
git push -u origin feature/whale-alert     # la primera vez se sube la branch así; después alcanza con git push
```

## Entorno virtual

Dentro de la carpeta del proyecto, crear y activar el entorno virtual.

### Windows

Crear el entorno virtual (solo la primera vez):

```bash
py -m venv .venv
```

Activar el entorno virtual:

```bash
.venv\Scripts\activate
```

### Linux

Crear el entorno virtual (solo la primera vez):

```bash
python3 -m venv .venv
```

Activar el entorno virtual:

```bash
source .venv/bin/activate
```

### Instalar paquetes

Ejecutar siempre con el entorno virtual activado (debe verse `(.venv)` a la izquierda de la consola):

```bash
pip install -r requirements.txt
```

## Código limpio y pre-commit

El código se lee muchas más veces de las que se escribe. Un código limpio (formato uniforme, nombres claros, errores, orden, etc) es más fácil de revisar, de mantener y de trabajar por terceros.
En un equipo, además, evita que los *pull requests* se llenen de cambios de espacios o de orden de imports
y deja la revisión para lo importante: la lógica.

[pre-commit](https://pre-commit.com) automatiza ese control: antes de cada `git commit` corre una lista de
herramientas (*hooks*) sobre los archivos stageados. Si alguna encuentra un problema, el commit se frena;
los formateadores corrigen el archivo solos, y alcanza con revisar el cambio, volver a hacer `git add` y repetir el commit.
Así los errores se detectan en la PC, antes de llegar al repositorio o a producción.

En este repo (`.pre-commit-config.yaml`) controla:

| Qué | Herramientas |
|---|---|
| Formato de Python | `black` (estilo), `isort` (orden de imports), `pycln` (imports sin usar) |
| Tipos de Python | `mypy` |
| SQL | `sqlfluff` (dialecto PostgreSQL) |
| YAML y JSON | `yamllint`, `check-yaml`, `check-json` |
| Higiene general | espacios al final, salto de línea final, marcas de conflicto de merge, `breakpoint()` olvidados, archivos grandes |
| Seguridad | `detect-secrets` (contraseñas o tokens subidos por error) |

Uso:

```bash
pip install pre-commit
pre-commit install          # activa el hook en este repo (solo la primera vez)
pre-commit run --all-files  # revisa todo el repo
pre-commit autoupdate       # actualiza las versiones de los hooks
```

Se recomienda sumarlo a cualquier proyecto, propio o de trabajo, desde el primer commit: alcanza con copiar
`.pre-commit-config.yaml`, quitar los hooks que no apliquen y ejecutar `pre-commit install`.
Agregarlo a un proyecto grande ya existente cuesta más, porque el primer `pre-commit run --all-files` reformatea todo.

## Docker

Toda la plataforma (Airflow, MinIO, PostgreSQL y pgAdmin) está en un único archivo: `docker-compose.yaml`.

1. Crear el archivo `.env` a partir del ejemplo (solo la primera vez):

   ```bash
   cp .env.example .env
   ```

   En Linux, poner el ID del usuario en `AIRFLOW_UID` (lo muestra `id -u`) para evitar problemas de permisos.

2. Levantar todo:

   ```bash
   docker compose up -d
   ```

   La primera vez tarda unos minutos (descarga imágenes e inicializa Airflow).

Comandos comunes:

```bash
docker compose ps            # estado de los servicios
docker compose logs -f minio # ver logs de un servicio
docker compose stop          # pausar (sin borrar contenedores)
docker compose start         # reanudar
docker compose down          # apagar y borrar contenedores (los datos quedan en los volúmenes)
docker compose down -v       # apagar y BORRAR también los datos
```

### Servicios

Todos los usuarios y contraseñas están en el `.env` (ver comentarios en `.env.example`).
Valores por defecto:

| Servicio | URL / host | Usuario | Contraseña |
|---|---|---|---|
| Airflow | http://localhost:8080 | airflow | airflow |
| MinIO (consola) | http://localhost:9001 | admin | password |
| MinIO (API S3) | http://localhost:9000 | admin | password |
| pgAdmin | http://localhost:5050 | admin@admin.com | admin123 |
| PostgreSQL DW `postgres_db` – base `local_db` | localhost:5432 | admin | admin123 |
| PostgreSQL Airflow `postgres_airflow` – base `airflow` | localhost:5433 | airflow | airflow |
| PostgreSQL Airflow `postgres_airflow` – base `amn` (origen) | localhost:5433 | amn | amn123 |

> MinIO usa la imagen `pgsty/minio` (versión de la comunidad): la oficial `minio/minio` ya no se publica.

> Dentro de Docker los servicios se llaman por su nombre (y usan el puerto 5432): en pgAdmin o en un DAG
> los hosts son `postgres_db`, `postgres_airflow` y `http://minio:9000`. Desde la PC es `localhost`.

### Las dos bases PostgreSQL 18

```
postgres_airflow (5433)                  postgres_db = data warehouse (5432)
├─ airflow  metadatos de Airflow         └─ local_db
└─ amn      sistema origen (auxiliar)       ├─ bronze   copia cruda de amn por día (append) y whale alert
     │                                      ├─ silver   datos limpios (upsert)
     └── DAGs amn_* (un día por corrida) ──▶├─ gold     modelo estrella (fact + dimensiones)
                                            └─ public   proyecto integrador 2
```

Airflow se conecta a las dos con las **Connections** `amn_origen` y `postgres_dw`
(se crean solas al levantar; se ven en *Admin → Connections*).

Los scripts de `ddl/` se ejecutan automáticamente **la primera vez que se crea cada volumen**:

- `ddl/postgres_airflow/`: crea el usuario y la base `amn`.
- `ddl/postgres_dw/`: crea las tablas bronze/silver de AMN, `bronze.whale_alerts_limits` y el modelo gold.
  Los DAGs también los ejecutan (`CREATE ... IF NOT EXISTS`) antes de cargar.

## DAGs de Airflow

| DAG | Archivo | Qué hace |
|---|---|---|
| `primer_dag_forma_vieja` | `dags/primer_dag_forma_vieja.py` | Ejemplo mínimo con `with DAG(...)` |
| `primer_dag_forma_nueva` | `dags/primer_dag_forma_nueva.py` | El mismo ejemplo con el decorador `@dag` |
| `whale_alert` | `dags/whale_alert.py` | Whale Alert → MinIO → PostgreSQL, con `PythonOperator` |
| `aux_amn_0_master_ingesta` | `dags/aux_amn_0_master_ingesta.py` | Dispara en orden los 3 DAGs seed y espera a que terminen |
| `aux_amn_1_data_ingestion_dag` | `dags/aux_amn_1_data_ingestion_dag.py` | Seed 1: descarga los ZIP de PwC al bucket `rawdata` |
| `aux_amn_2_data_transformation_dag` | `dags/aux_amn_2_data_transformation_dag.py` | Seed 2: ZIP de `rawdata` → CSV en el bucket `staging` |
| `aux_amn_3_database_loader_dag` | `dags/aux_amn_3_database_loader_dag.py` | Seed 3: CSV de `staging` → base origen `amn` |
| `amn_purchases` | `dags/amn_purchases_dag.py` | Compras por `PODate`, marzo y abril 2016 día por día: amn → datalake → bronze → silver |
| `amn_vendor_invoices` | `dags/amn_vendor_invoices_dag.py` | Facturas por `InvoiceDate`, marzo y abril 2016 día por día: amn → datalake → bronze → silver |
| `amn_purchase_prices` | `dags/amn_purchase_prices_dag.py` | Lista de precios (snapshot `@once`, no tiene fecha): amn → datalake → bronze → silver |
| `amn_beginv` | `dags/amn_beginv_dag.py` | Inventario inicial: una corrida, el día 2016-01-01: amn → datalake → bronze → silver |
| `amn_endinv` | `dags/amn_endinv_dag.py` | Inventario final: una corrida, el día 2016-12-31: amn → datalake → bronze → silver |
| `gold_seed_dim_tiempo` | `dags/gold_seed_dim_tiempo_dag.py` | Carga `gold.dim_tiempo` (2015 a 2027) una sola vez |
| `gold_amn_purchases_model` | `dags/gold_amn_purchases_model_dag.py` | Modelo estrella de compras; se dispara por **assets** de silver |

### Cómo correr AMN

1. **Seed**: activar `aux_amn_1_data_ingestion_dag`, `aux_amn_2_data_transformation_dag` y
   `aux_amn_3_database_loader_dag`, y disparar a mano `aux_amn_0_master_ingesta`, que los corre en orden. Deja los datos de PwC en la base origen `amn`.
2. **Activar** los 5 DAGs `amn_*`. Cada uno tiene 3 tareas:
   - `extraer_a_datalake`: `SELECT` en `amn` de las filas del intervalo de la corrida
     (`data_interval_start` ≤ fecha < `data_interval_end`; el `SELECT` exacto queda en el log de la tarea) y las guarda como **Parquet** en el bucket `datalake`,
     particionado igual que whale alert: `source=amn/dataset=<tabla>/year=2016/month=03/20160301T000000_<tabla>.parquet`.
   - `datalake_a_bronze`: lee ese Parquet (la ruta llega por XCom) y lo **agrega** a `bronze.amn_<tabla>`
     con columnas de metadatos.
   - `bronze_a_silver`: SQL que limpia lo que cargó esa corrida (tipos, `TRIM`, nulos, valores negativos, duplicados)
     y hace **upsert** (`INSERT ... ON CONFLICT DO UPDATE`) en `silver.amn_<tabla>`. Si un día se cargó más de una vez,
     se queda con la carga más nueva: `ROW_NUMBER() OVER (PARTITION BY <primary key> ORDER BY _created_at DESC) = 1`.
     Al terminar publica el **asset** de esa tabla silver.

   `amn_purchases` y `amn_vendor_invoices` procesan **marzo y abril de 2016** (dos meses de ejemplo):
   con `CronDataIntervalTimetable("@daily")` y `catchup=True` Airflow crea 61 corridas, cada una con un intervalo
   de un día (la del 1/3 cubre `[2016-03-01 00:00, 2016-03-02 00:00)`), con hasta 3 en paralelo (`max_active_runs=3`).
   Los sábados no hay compras, así que esas corridas cargan 0 filas.
   `amn_beginv` y `amn_endinv` tienen una sola corrida, la del día de su foto (2016-01-01 y 2016-12-31).
   `amn_purchase_prices` no tiene ninguna fecha: corre una sola vez (`@once`) y extrae la tabla completa.
3. **Gold**: activar primero `gold_amn_purchases_model` y después `gold_seed_dim_tiempo` (corre una sola vez).
   `gold_amn_purchases_model` no tiene horario: se dispara cada vez que se actualiza alguno de los assets de silver que usa (todos menos `amn_vendor_invoices`) o `dim_tiempo`
   (ver la vista *Assets* de la UI). Mientras está pausado, Airflow descarta esos eventos: si no corrió, dispararlo a mano.
4. Probar la idempotencia: limpiar (*Clear*) un día ya procesado. En bronze ese día queda repetido,
   en silver y en gold la cantidad de filas no cambia.

### Columnas de metadatos

| Columna | bronze | silver | gold | Qué guarda |
|---|:-:|:-:|:-:|---|
| `_s3_path` | ✓ | ✓ | | Parquet del datalake del que salió la fila, ej. `s3://datalake/source=amn/dataset=purchases/...` |
| `_s3_version` | ✓ | ✓ | | `VersionId` de ese Parquet: el bucket `datalake` tiene versionado, al re-ejecutar un día se crea una versión nueva |
| `_created_by` | ✓ | ✓ | ✓ | Corrida de Airflow que creó la fila: `airflow__<dag_id>__<run_id>` |
| `_created_at` | ✓ | ✓ | ✓ | Cuándo se creó |
| `_updated_by` | | ✓ | ✓ | Última corrida que la actualizó (el upsert no toca `_created_*`) |
| `_updated_at` | | ✓ | ✓ | Cuándo se actualizó por última vez |
| `_data_interval_start` / `_end` | ✓ | ✓ | | Intervalo de datos de la corrida que la cargó |

`_s3_path` y `_s3_version` los escribe `datalake_a_bronze` y viajan hasta silver (linaje).
Si las bases se crearon con una versión anterior de las tablas, recrearlas con
`docker compose down -v` (borra los datos) y `docker compose up -d`.

### Modelo gold (estrella)

```
                     dim_tiempo  (fecha de orden y de recepción)
                          │
   dim_producto ──── fact_compras ──── dim_proveedor
                          │
                     dim_tienda
```

- **fact_compras**: una fila por línea de compra recibida. Medidas: `cantidad`, `precio_compra_unitario`,
  `importe_compra`, `precio_venta_unitario` y `margen_estimado` (cantidad × (precio de venta − precio de compra)).
- **dim_producto** (marca), **dim_proveedor**, **dim_tienda** (ciudad), **dim_tiempo** (día, mes, trimestre,
  día de la semana, fin de semana; nombres en español).

Ejemplo, los productos con más margen:

```sql
SELECT p.descripcion, SUM(f.cantidad) AS unidades, SUM(f.margen_estimado) AS margen
FROM gold.fact_compras f
JOIN gold.dim_producto p USING (producto_key)
GROUP BY p.descripcion
ORDER BY margen DESC NULLS LAST
LIMIT 10;
```

Las conexiones a MinIO y al PostgreSQL de whale alert están en `plugins/utils/connections.py`;
las funciones comunes de AMN, en `plugins/utils/layer.py`, y los assets, en `plugins/utils/assets.py`.

## Problemas comunes

- **`column "..." does not exist` o tablas con columnas viejas**: los scripts de `ddl/` sólo corren al crear
  el volumen y usan `CREATE TABLE IF NOT EXISTS`, así que no modifican tablas que ya existen.
  Recrear todo con `docker compose down -v` (borra los datos) y `docker compose up -d`.
- **Se cambió el `schedule` de un DAG y las corridas viejas no traen datos**: cada corrida guarda el intervalo
  con el que se creó. Al cambiar el timetable hay que borrar esas corridas (*Browse → DAG Runs*) y
  volver a crearlas con un backfill (*Trigger → Backfill* en la UI, o `airflow backfill create`).
- **Contenedores que se reinician o salen con código 137**: les falta memoria. Asignar al menos
  **8 GB** a Docker Desktop (*Settings → Resources*). El worker corre hasta 4 tareas a la vez
  (`AIRFLOW__CELERY__WORKER_CONCURRENCY` en `docker-compose.yaml`).
- **Correr el seed otra vez**: se puede con los DAGs diarios activos. El loader carga cada tabla en
  `<tabla>_nueva` y la reemplaza al final, así nunca se lee una tabla a medio cargar.

## Scripts de los proyectos integradores

Con el entorno virtual activado y `docker compose up -d` corriendo:

```bash
# Scraping de práctica: guarda CSV/JSON/Parquet en data/ (ejecutar desde la raíz del repo)
python python-scripts/whale-alerts.py

# Proyecto integrador 1: Whale Alert → MinIO (sin Airflow)
python python-scripts/proyecto-integrador-1/whale-alerts-borrador-script.py

# Proyecto integrador 2: crea tablas y carga los CSV en PostgreSQL
cd python-scripts/proyecto-integrador-2/src
python main.py
```

## Estructura

```
dags/              DAGs de Airflow
plugins/           Código compartido por los DAGs (plugins/utils)
ddl/               Scripts que se ejecutan al crear cada base (postgres_airflow y postgres_dw)
config/            Configuración de Airflow (airflow.cfg)
python-scripts/    Scraping de práctica y scripts de los proyectos integradores
docker-compose.yaml
.env.example       Variables de entorno de ejemplo (copiar a .env)
```

## Arquitectura

Mapa de la arquitectura de la plataforma de datos:
https://app.diagrams.net/#G1j_DEIspJAXuBSPLlezS1XO-OoCya_XeN


## Imágenes de conceptos de Data Engineering
https://drive.google.com/drive/folders/1YK69J-4IXyazXimfA_jTiwEuz7rtj_qq?usp=sharing
