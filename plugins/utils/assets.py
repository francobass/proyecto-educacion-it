"""Assets (datasets) de Airflow que conectan los DAGs AMN por datos, no por horario.

Una tarea que tiene un asset en `outlets` lo "actualiza" al terminar bien (aunque después falle
otra tarea del DAG); un DAG con `schedule=<asset>` se dispara solo cuando ese asset se actualiza.
Se ven en la UI en "Assets".
La URI sigue el formato del provider de Postgres: postgres://host:puerto/base/esquema/tabla.
(Airflow no se conecta a esa URI: es solo el nombre que une `outlets` con `schedule`).
"""

from airflow.sdk import Asset

_DW = "postgres://postgres_db:5432/local_db"

# Silver: los publican las tareas bronze_a_silver de los DAGs amn_*
SILVER_PURCHASES = Asset(f"{_DW}/silver/amn_purchases")
SILVER_VENDOR_INVOICES = Asset(f"{_DW}/silver/amn_vendor_invoices")
SILVER_PURCHASE_PRICES = Asset(f"{_DW}/silver/amn_purchase_prices")
SILVER_BEGINV = Asset(f"{_DW}/silver/amn_beginv")
SILVER_ENDINV = Asset(f"{_DW}/silver/amn_endinv")

# Gold: modelo estrella
GOLD_DIM_TIEMPO = Asset(f"{_DW}/gold/dim_tiempo")
GOLD_DIM_PRODUCTO = Asset(f"{_DW}/gold/dim_producto")
GOLD_DIM_PROVEEDOR = Asset(f"{_DW}/gold/dim_proveedor")
GOLD_DIM_TIENDA = Asset(f"{_DW}/gold/dim_tienda")
GOLD_FACT_COMPRAS = Asset(f"{_DW}/gold/fact_compras")
