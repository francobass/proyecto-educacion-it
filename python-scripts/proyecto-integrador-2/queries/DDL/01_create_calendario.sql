CREATE TABLE IF NOT EXISTS Calendario (
    IdFecha INTEGER PRIMARY KEY,
    fecha DATE NOT NULL,
    anio INTEGER NOT NULL,
    mes INTEGER NOT NULL,
    dia INTEGER NOT NULL,
    trimestre INTEGER NOT NULL,
    semana INTEGER NOT NULL,
    dia_nombre VARCHAR(20) NOT NULL,
    mes_nombre VARCHAR(20) NOT NULL
);