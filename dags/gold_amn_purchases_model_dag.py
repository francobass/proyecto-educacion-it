"""DAG gold AMN: arma el modelo estrella de compras a partir de silver.

    crear_tablas -> verificar_dim_tiempo -> [dim_producto, dim_proveedor, dim_tienda] -> fact_compras

No tiene horario: se dispara por ASSETS cuando se actualiza cualquiera de las tablas silver
que usa (amn_purchases, amn_purchase_prices, amn_beginv, amn_endinv) o la dimensión tiempo.
Cada corrida recalcula todo desde silver con upsert, así que se puede repetir sin duplicar.
Cada fila guarda qué corrida la creó y cuál la actualizó por última vez (_created_* / _updated_*).
Requiere haber corrido gold_seed_dim_tiempo (lo controla verificar_dim_tiempo).
Ojo: mientras este DAG está pausado, Airflow descarta los eventos de sus assets (no quedan en cola).
Activarlo antes que gold_seed_dim_tiempo o, si ya corrió, dispararlo a mano con Trigger.
"""

from datetime import datetime

from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG
from utils.assets import (
    GOLD_DIM_PRODUCTO,
    GOLD_DIM_PROVEEDOR,
    GOLD_DIM_TIEMPO,
    GOLD_DIM_TIENDA,
    GOLD_FACT_COMPRAS,
    SILVER_BEGINV,
    SILVER_ENDINV,
    SILVER_PURCHASE_PRICES,
    SILVER_PURCHASES,
)
from utils.layer import CONN_DW, CREATED_BY, ejecutar_ddl

# %(created_by)s en los SQL: Airflow lo completa con "airflow__<dag_id>__<run_id>" (_created_by / _updated_by)
PARAMS = {"created_by": CREATED_BY}

# Producto: la lista de precios tiene más datos; si una marca no está, se toma de las compras
DIM_PRODUCTO = """
-- ROW_NUMBER: una fila por marca. Gana la lista de precios (prioridad 1, tiene más datos);
-- si la marca no está, se toma de las compras. A igual prioridad, la cargada más recientemente (_created_at);
-- las filas de una misma corrida empatan y queda cualquiera (alcanza para descripción y tamaño).
WITH productos AS (
    SELECT brand, description, size, volume_ml, classification, price, purchase_price, margin,
           1 AS prioridad, _created_at
    FROM silver.amn_purchase_prices
    UNION ALL
    SELECT brand, description, size, NULL, classification, NULL, purchase_price, NULL,
           2 AS prioridad, _created_at
    FROM silver.amn_purchases
),
ultimas AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY brand ORDER BY prioridad, _created_at DESC) AS rn
    FROM productos
    WHERE brand IS NOT NULL
)
INSERT INTO gold.dim_producto (
    producto_key, descripcion, tamanio, volumen_ml, clasificacion,
    precio_venta, precio_compra, margen_unitario, _created_by, _created_at, _updated_by, _updated_at
)
SELECT brand, description, size, volume_ml, classification, price, purchase_price, margin,
       %(created_by)s, now(), %(created_by)s, now()
FROM ultimas
WHERE rn = 1
ON CONFLICT (producto_key) DO UPDATE SET
    descripcion     = EXCLUDED.descripcion,
    tamanio         = EXCLUDED.tamanio,
    volumen_ml      = EXCLUDED.volumen_ml,
    clasificacion   = EXCLUDED.clasificacion,
    precio_venta    = EXCLUDED.precio_venta,
    precio_compra   = EXCLUDED.precio_compra,
    margen_unitario = EXCLUDED.margen_unitario,
    _updated_by     = EXCLUDED._updated_by,
    _updated_at     = EXCLUDED._updated_at;
"""

# Proveedor: puede aparecer en compras y en la lista de precios
DIM_PROVEEDOR = """
-- ROW_NUMBER: una fila por proveedor (primero el nombre de las compras, después el de la lista de precios)
WITH proveedores AS (
    SELECT vendor_number, vendor_name, 1 AS prioridad, _created_at FROM silver.amn_purchases
    UNION ALL
    SELECT vendor_number, vendor_name, 2 AS prioridad, _created_at FROM silver.amn_purchase_prices
),
ultimas AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY vendor_number ORDER BY prioridad, _created_at DESC) AS rn
    FROM proveedores
    WHERE vendor_number IS NOT NULL
)
INSERT INTO gold.dim_proveedor (proveedor_key, nombre, _created_by, _created_at, _updated_by, _updated_at)
SELECT vendor_number, vendor_name, %(created_by)s, now(), %(created_by)s, now()
FROM ultimas
WHERE rn = 1
ON CONFLICT (proveedor_key) DO UPDATE SET
    nombre          = EXCLUDED.nombre,
    _updated_by     = EXCLUDED._updated_by,
    _updated_at     = EXCLUDED._updated_at;
"""

# Tienda: la ciudad sale de los inventarios o del inventory_id de las compras ('69_MOUNTMEND_8412')
DIM_TIENDA = """
-- ROW_NUMBER: una fila por tienda, priorizando las filas que sí tienen ciudad
WITH tiendas AS (
    SELECT store, city AS ciudad, _created_at FROM silver.amn_beginv
    UNION ALL
    SELECT store, city, _created_at FROM silver.amn_endinv
    UNION ALL
    SELECT store, NULLIF(SPLIT_PART(inventory_id, '_', 2), ''), _created_at FROM silver.amn_purchases
),
ultimas AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY store ORDER BY ciudad IS NULL, _created_at DESC) AS rn
    FROM tiendas
    WHERE store IS NOT NULL
)
INSERT INTO gold.dim_tienda (tienda_key, ciudad, _created_by, _created_at, _updated_by, _updated_at)
SELECT store, ciudad, %(created_by)s, now(), %(created_by)s, now()
FROM ultimas
WHERE rn = 1
ON CONFLICT (tienda_key) DO UPDATE SET
    ciudad          = EXCLUDED.ciudad,
    _updated_by     = EXCLUDED._updated_by,
    _updated_at     = EXCLUDED._updated_at;
"""

# Hechos: una fila por línea de compra, con las claves de las dimensiones y las medidas
FACT_COMPRAS = """
INSERT INTO gold.fact_compras (
    fecha_orden_key, numero_orden, inventory_id, fecha_recepcion_key,
    producto_key, proveedor_key, tienda_key,
    cantidad, precio_compra_unitario, importe_compra, precio_venta_unitario, margen_estimado,
    _created_by, _created_at, _updated_by, _updated_at
)
SELECT
    TO_CHAR(p.po_date, 'YYYYMMDD')::integer,         -- clave de dim_tiempo (AAAAMMDD)
    p.po_number,
    p.inventory_id,
    TO_CHAR(p.receiving_date, 'YYYYMMDD')::integer,
    p.brand,
    p.vendor_number,
    p.store,
    p.quantity,
    p.purchase_price,
    p.dollars,
    dp.precio_venta,
    ROUND(p.quantity * (dp.precio_venta - p.purchase_price), 2),  -- NULL si no hay precio de venta
    %(created_by)s, now(), %(created_by)s, now()
FROM silver.amn_purchases AS p
LEFT JOIN gold.dim_producto AS dp ON dp.producto_key = p.brand
-- silver.amn_purchases ya tiene una fila por clave: no hace falta ROW_NUMBER acá
ON CONFLICT (numero_orden, inventory_id, fecha_recepcion_key) DO UPDATE SET
    fecha_orden_key        = EXCLUDED.fecha_orden_key,
    producto_key           = EXCLUDED.producto_key,
    proveedor_key          = EXCLUDED.proveedor_key,
    tienda_key             = EXCLUDED.tienda_key,
    cantidad               = EXCLUDED.cantidad,
    precio_compra_unitario = EXCLUDED.precio_compra_unitario,
    importe_compra         = EXCLUDED.importe_compra,
    precio_venta_unitario  = EXCLUDED.precio_venta_unitario,
    margen_estimado        = EXCLUDED.margen_estimado,
    _updated_by            = EXCLUDED._updated_by,
    _updated_at            = EXCLUDED._updated_at;
"""


with DAG(
    "gold_amn_purchases_model",
    description="Modelo estrella de compras AMN (gold), disparado por assets de silver",
    # El operador | significa "cualquiera de estos assets": basta con que se actualice uno
    schedule=(
        SILVER_PURCHASES
        | SILVER_PURCHASE_PRICES
        | SILVER_BEGINV
        | SILVER_ENDINV
        | GOLD_DIM_TIEMPO
    ),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1,  # una corrida a la vez: los backfill de silver disparan muchas
):
    crear_tablas = PythonOperator(
        task_id="crear_tablas",
        python_callable=ejecutar_ddl,
        op_kwargs={"archivo": "20_gold.sql"},
    )

    # SQLCheckOperator falla si la consulta devuelve false/0: avisa que falta el seed
    verificar_dim_tiempo = SQLCheckOperator(
        task_id="verificar_dim_tiempo",
        conn_id=CONN_DW,
        sql="SELECT COUNT(*) > 0 FROM gold.dim_tiempo  -- si falla: correr gold_seed_dim_tiempo",
    )

    dim_producto = SQLExecuteQueryOperator(
        task_id="dim_producto",
        conn_id=CONN_DW,
        sql=DIM_PRODUCTO,
        parameters=PARAMS,
        outlets=[GOLD_DIM_PRODUCTO],
    )
    dim_proveedor = SQLExecuteQueryOperator(
        task_id="dim_proveedor",
        conn_id=CONN_DW,
        sql=DIM_PROVEEDOR,
        parameters=PARAMS,
        outlets=[GOLD_DIM_PROVEEDOR],
    )
    dim_tienda = SQLExecuteQueryOperator(
        task_id="dim_tienda",
        conn_id=CONN_DW,
        sql=DIM_TIENDA,
        parameters=PARAMS,
        outlets=[GOLD_DIM_TIENDA],
    )

    # La fact va al final: sus claves foráneas necesitan las dimensiones cargadas
    fact_compras = SQLExecuteQueryOperator(
        task_id="fact_compras",
        conn_id=CONN_DW,
        sql=FACT_COMPRAS,
        parameters=PARAMS,
        outlets=[GOLD_FACT_COMPRAS],
    )

    (
        crear_tablas
        >> verificar_dim_tiempo
        >> [dim_producto, dim_proveedor, dim_tienda]
        >> fact_compras
    )
