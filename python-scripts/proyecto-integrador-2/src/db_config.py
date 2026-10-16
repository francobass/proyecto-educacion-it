"""Datos de conexión a PostgreSQL, compartidos por create_tables.py y load_data.py.

Por defecto apunta al postgres_db del docker-compose visto desde la PC (localhost:5432).
Se pueden cambiar con variables de entorno (POSTGRES_HOST, POSTGRES_PORT, ...).
Este script no lee el .env (eso lo hace docker compose): si se cambia usuario o contraseña,
definir las mismas variables (POSTGRES_USER, POSTGRES_PASSWORD, ...) antes de correr main.py.
"""

import os

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "database": os.getenv("POSTGRES_DB", "local_db"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "admin123"),
}

# Misma conexión en formato URL, para SQLAlchemy
DB_URL = (
    f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
    f"@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
)
