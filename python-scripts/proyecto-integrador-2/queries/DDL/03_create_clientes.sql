-- Crear tabla Clientes
CREATE TABLE IF NOT EXISTS Clientes (
    ID INTEGER PRIMARY KEY,
    Provincia VARCHAR(100),
    Nombre_y_Apellido VARCHAR(200) NOT NULL,
    Domicilio VARCHAR(500),
    Telefono VARCHAR(50),
    Edad INTEGER,
    Localidad VARCHAR(100),
    X DECIMAL(15,8), -- Longitud
    Y DECIMAL(15,8), -- Latitud
    Fecha_Alta DATE NOT NULL,
    Usuario_Alta VARCHAR(100) NOT NULL,
    Fecha_Ultima_Modificacion DATE NOT NULL,
    Usuario_Ultima_Modificacion VARCHAR(100) NOT NULL,
    Marca_Baja INTEGER DEFAULT 0,
    col10 VARCHAR(255) -- Columna adicional del CSV
);
