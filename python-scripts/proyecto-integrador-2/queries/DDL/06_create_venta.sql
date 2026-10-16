-- Crear tabla Venta
CREATE TABLE IF NOT EXISTS Venta (
    IdVenta INTEGER PRIMARY KEY,
    Fecha DATE NOT NULL,
    Fecha_Entrega DATE NOT NULL,
    IdCanal INTEGER NOT NULL,
    IdCliente INTEGER NOT NULL,
    IdSucursal INTEGER NOT NULL,
    IdEmpleado INTEGER NOT NULL,
    IdProducto INTEGER NOT NULL,
    Precio DECIMAL(10,2),       -- puede venir vacío en el CSV (dato crudo)
    Cantidad INTEGER            -- puede venir vacía en el CSV (dato crudo)
);
