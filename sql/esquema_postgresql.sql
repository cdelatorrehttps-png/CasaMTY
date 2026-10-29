-- CasaMTY - Esquema estrella para PostgreSQL (warehouse escolar definido en la Fase 2)
-- Uso:
--   createdb casamty
--   psql -d casamty -f sql/esquema_postgresql.sql
--   python src/etapa5_warehouse.py --url postgresql+psycopg2://usuario:clave@localhost:5432/casamty
-- (el script crea las tablas con pandas; este DDL sirve de referencia, para tipos estrictos y para los índices)

CREATE TABLE IF NOT EXISTS dim_ubicacion (
    id_ubicacion INT PRIMARY KEY,
    municipio    TEXT NOT NULL,
    colonia      TEXT NOT NULL,
    zona         TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_inmueble_tipo (
    id_tipo      INT PRIMARY KEY,
    tipo         TEXT NOT NULL,           -- casa / departamento / terreno
    es_vivienda  BOOLEAN NOT NULL
);
CREATE TABLE IF NOT EXISTS dim_operacion (
    id_operacion INT PRIMARY KEY,
    operacion    TEXT NOT NULL            -- venta / renta  (NUNCA se mezclan en un KPI de precio)
);
CREATE TABLE IF NOT EXISTS dim_fuente (
    id_fuente           INT PRIMARY KEY,
    fuente              TEXT NOT NULL,
    tipo_extraccion     TEXT,
    fecha_ultima_corrida DATE
);

CREATE TABLE IF NOT EXISTS hecho_anuncio (
    id_anuncio_global BIGINT,
    id_fuente        INT REFERENCES dim_fuente(id_fuente),
    id_ubicacion     INT REFERENCES dim_ubicacion(id_ubicacion),
    id_tipo          INT REFERENCES dim_inmueble_tipo(id_tipo),
    id_operacion     INT REFERENCES dim_operacion(id_operacion),
    fecha_captura    DATE,
    fecha_publicacion DATE,
    moneda           TEXT,
    precio_mxn       NUMERIC(14,2),
    m2               NUMERIC(10,1),
    recamaras        INT,
    banos            INT,
    estacionamientos INT,
    precio_m2        NUMERIC(12,2),
    flag_completo    BOOLEAN,
    flag_duplicado   BOOLEAN,
    tipo_duplicado   TEXT,                -- recaptura / interportal
    flag_outlier     BOOLEAN,
    flag_usable      BOOLEAN              -- completo, no duplicado, no atípico, no terreno
);

CREATE TABLE IF NOT EXISTS hecho_estimacion (
    id_ubicacion  INT REFERENCES dim_ubicacion(id_ubicacion),
    id_tipo       INT REFERENCES dim_inmueble_tipo(id_tipo),
    id_operacion  INT REFERENCES dim_operacion(id_operacion),
    n_anuncios_usados            INT,
    precio_m2_mediana_observada  NUMERIC(12,2),
    precio_m2_estimado_modelo    NUMERIC(12,2),
    n_prueba                     INT,
    mape_zona                    NUMERIC(8,2),
    suficiente_muestra           BOOLEAN,
    cumple_meta_mape             BOOLEAN,
    fecha_modelo                 DATE
);

CREATE INDEX IF NOT EXISTS ix_h_ub        ON hecho_anuncio(id_ubicacion);
CREATE INDEX IF NOT EXISTS ix_h_op_tipo   ON hecho_anuncio(id_operacion, id_tipo);
CREATE INDEX IF NOT EXISTS ix_h_usable    ON hecho_anuncio(flag_usable) WHERE flag_usable;
CREATE INDEX IF NOT EXISTS ix_h_fecha     ON hecho_anuncio(fecha_captura);
