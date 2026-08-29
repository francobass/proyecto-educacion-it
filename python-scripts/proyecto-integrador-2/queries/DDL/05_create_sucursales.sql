-- Crear tabla Sucursales
CREATE TABLE IF NOT EXISTS Sucursales (
    ID INTEGER PRIMARY KEY,
    Sucursal VARCHAR(100) NOT NULL UNIQUE,
    Direccion VARCHAR(300) NOT NULL,
    Localidad VARCHAR(100) NOT NULL,
    Provincia VARCHAR(100) NOT NULL,
    Latitud DECIMAL(15,8) NOT NULL,
    Longitud DECIMAL(15,8) NOT NULL
);
