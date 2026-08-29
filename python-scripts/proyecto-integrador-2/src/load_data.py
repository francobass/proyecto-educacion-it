import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'local_db',
    'user': 'admin',
    'password': 'admin123'
}

def load_data():
    data_dir = Path(__file__).parent.parent / 'data'
    
    engine = create_engine(f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    print("Cargando Calendario")
    df_calendario = pd.read_csv(data_dir / 'Calendario.csv')
    df_calendario.to_sql('calendario', engine, if_exists='replace', index=False)
    print("Calendario cargado")
    
    print("Cargando CanalDeVenta")
    df_canal = pd.read_csv(data_dir / 'CanalDeVenta.csv')
    df_canal.to_sql('canaldeventa', engine, if_exists='replace', index=False)
    print("CanalDeVenta cargado")
    
    print("Cargando Clientes")
    df_clientes = pd.read_csv(data_dir / 'Clientes.csv', sep=';')
    df_clientes.to_sql('clientes', engine, if_exists='replace', index=False)
    print("Clientes cargado")

    print("Cargando Productos")
    df_productos = pd.read_csv(data_dir / 'Productos.csv')
    df_productos.to_sql('productos', engine, if_exists='replace', index=False)
    print("Productos cargado")
    
    print("Cargando Sucursales")
    df_sucursales = pd.read_csv(data_dir / 'Sucursales.csv', sep=';')
    df_sucursales.to_sql('sucursales', engine, if_exists='replace', index=False)
    print("Sucursales cargado")
    
    print("Cargando Venta")
    df_venta = pd.read_csv(data_dir / 'Venta.csv')
    df_venta.to_sql('venta', engine, if_exists='replace', index=False)
    print("Venta cargado")
    
    engine.dispose()
    print("Todos los datos cargados")