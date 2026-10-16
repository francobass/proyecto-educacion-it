-- Crear tabla Productos
CREATE TABLE IF NOT EXISTS Productos (
    ID_PRODUCTO INTEGER PRIMARY KEY,
    Concepto VARCHAR(500) NOT NULL,
    Tipo VARCHAR(100),
    Precio DECIMAL(12,2) NOT NULL  -- el CSV trae precios atípicos (ej. 244444442.00)
);
