"""
CasaMTY - Configuración central del proyecto (Fase 3, MVP).

Aquí viven las rutas, parámetros de la simulación y el catálogo de municipios /
colonias del AMM. TODOS los datos que genera el proyecto son SIMULADOS con fines
académicos (la guía de la Fase 3 permite dataset simulado o real). Los factores
de precio por colonia son supuestos de diseño, NO estadísticas oficiales.
"""
import os
from pathlib import Path

# --------------------------------------------------------------------------- #
# Rutas
# --------------------------------------------------------------------------- #
RAIZ = Path(__file__).resolve().parent.parent
DATA = RAIZ / "data"
LAKE = DATA / "lake"
BRONCE = LAKE / "bronce"      # crudo: JSONL.gz tal como "salió" de cada portal
PLATA = LAKE / "plata"        # parseado y estandarizado (Parquet)
ORO = LAKE / "oro"            # tablas de negocio / modelo estrella (Parquet)
METRICAS = DATA / "metricas"
WAREHOUSE_SQLITE = DATA / "warehouse" / "casamty.sqlite"
POWERBI = RAIZ / "powerbi"
DASHBOARD = RAIZ / "dashboard"
DOCS = RAIZ / "docs"
IMG = DOCS / "img"

# --------------------------------------------------------------------------- #
# Parámetros de simulación / pipeline
# --------------------------------------------------------------------------- #
# Registros crudos a generar (la guía pide >= 1 millón). Se puede cambiar con la
# variable de entorno CASAMTY_N para pruebas rápidas, p. ej. CASAMTY_N=50000
N_REGISTROS_CRUDOS = int(os.environ.get("CASAMTY_N", 1_500_000))
SEMILLA = 2026
CORRIDAS = ["2026-10-05", "2026-10-12", "2026-10-19"]   # 3 corridas semanales
FX_USD_MXN = 18.0   # supuesto fijo para convertir anuncios en USD

# Participación de cada portal en el volumen de anuncios
PORTALES = {
    "inmuebles24": 0.35,
    "lamudi": 0.25,
    "vivanuncios": 0.20,
    "mercadolibre": 0.20,
}

# Proporciones de "suciedad" inyectada (para poder medir Veracidad)
P_RECAPTURA = 0.18          # anuncio que reaparece en la corrida siguiente (mismo portal e id)
P_DUP_INTERPORTAL = 0.10    # mismo inmueble publicado en otro portal
P_M2_FALTANTE = 0.07
P_M2_INFLADO = 0.03
P_PRECIO_FALTANTE = 0.01
P_RENTA_COMO_VENTA = 0.008  # precio mensual de renta capturado en un anuncio de venta
P_REC_FALTANTE = 0.10
P_BANOS_FALTANTE = 0.12
P_ESTAC_FALTANTE = 0.20

# Reglas de calidad / negocio
M2_VIVIENDA = (25, 2000)
M2_TERRENO = (50, 20000)
FACTOR_OUTLIER = 3.0        # precio/m2 fuera de [mediana/3, mediana*3] => outlier
UMBRAL_MUESTRA_ZONA = 30    # anuncios mínimos para considerar "suficiente muestra"
META_MAPE = 20.0            # % (meta escolar de la fase 1/2)
META_COMPLETITUD = 70.0     # %
META_PIPELINE_MIN = 30.0    # minutos

# Cortes para el cubo del dashboard
M2_BUCKETS = [(0, 60, "<60"), (60, 90, "60-89"), (90, 120, "90-119"),
              (120, 180, "120-179"), (180, 250, "180-249"), (250, 10**9, "250+")]

# --------------------------------------------------------------------------- #
# Catálogo AMM (SIMULADO): municipio -> base de precio/m2 de venta, peso en el
# volumen de anuncios, alias de escritura y colonias (nombre, factor, zona, peso)
# --------------------------------------------------------------------------- #
MUNICIPIOS = {
    "Monterrey": {
        "base_ppm2": 31000, "peso": 0.22,
        "aliases": ["Monterrey, N.L.", "Monterrey N.L.", "MTY", "Monterrey, Nuevo León"],
        "colonias": [
            ("Cumbres 1er Sector", 0.85, "Cumbres", 1.2), ("Cumbres 2do Sector", 0.88, "Cumbres", 1.2),
            ("Cumbres 3er Sector", 0.92, "Cumbres", 1.4), ("Cumbres 4to Sector", 0.90, "Cumbres", 1.0),
            ("Cumbres Élite", 1.10, "Cumbres", 0.8), ("Contry", 1.25, "Sur", 1.0),
            ("Tecnológico", 1.05, "Sur", 1.0), ("Mitras Centro", 0.90, "Centro", 0.8),
            ("Centro Monterrey", 0.80, "Centro", 0.8), ("Obispado", 1.00, "Poniente", 0.7),
            ("Independencia", 0.60, "Centro", 0.6), ("Valle Alto", 1.15, "Sur", 0.8),
            ("Vista Hermosa", 1.20, "Sur", 0.7), ("Colonial La Silla", 0.95, "Sur", 0.8),
        ]},
    "San Pedro Garza García": {
        "base_ppm2": 58000, "peso": 0.07,
        "aliases": ["San Pedro", "SPGG", "San Pedro Garza Garcia", "San Pedro G.G."],
        "colonias": [
            ("Del Valle", 1.20, "Valle", 1.5), ("Valle Oriente", 1.25, "Valle", 1.2),
            ("Residencial San Agustín", 1.30, "Valle", 1.0), ("Fuentes del Valle", 1.00, "Valle", 1.0),
            ("Santa Engracia", 1.15, "Valle", 0.8), ("Lomas del Valle", 1.10, "Valle", 0.9),
            ("Chipinque", 1.35, "Sierra", 0.5), ("Los Sabinos", 0.80, "Centro SP", 0.8),
            ("Palo Blanco", 0.85, "Centro SP", 0.7), ("Rincón de San Pedro", 0.78, "Centro SP", 0.6),
            ("Colonia Jardines de San Agustín", 1.18, "Valle", 0.6), ("Misión San Pedro", 0.90, "Centro SP", 0.5),
        ]},
    "Guadalupe": {
        "base_ppm2": 20000, "peso": 0.10,
        "aliases": ["Guadalupe, N.L.", "Cd. Guadalupe", "Ciudad Guadalupe"],
        "colonias": [
            ("Linda Vista", 0.95, "Oriente", 1.0), ("Contry Sol", 1.15, "Oriente", 1.0),
            ("Guadalupe Centro", 0.85, "Centro", 0.9), ("Zertuche", 0.90, "Oriente", 0.8),
            ("Lomas de Guadalupe", 1.00, "Oriente", 0.9), ("Francisco Villa", 0.70, "Oriente", 0.8),
            ("Bosques del Valle", 1.05, "Oriente", 0.8), ("Jardines de Guadalupe", 0.95, "Oriente", 0.9),
            ("Mitras Oriente", 0.75, "Oriente", 0.7), ("Arboledas de Guadalupe", 0.85, "Oriente", 0.8),
            ("Valle del Mirador", 1.10, "Oriente", 0.7), ("Plaza Chapultepec", 0.80, "Oriente", 0.6),
        ]},
    "San Nicolás de los Garza": {
        "base_ppm2": 21000, "peso": 0.10,
        "aliases": ["San Nicolas", "San Nicolás", "San Nicolas de los Garza", "S. Nicolás"],
        "colonias": [
            ("Anáhuac", 1.10, "Norte", 1.2), ("Las Puentes", 0.90, "Norte", 1.0),
            ("Hacienda Los Morales", 1.20, "Norte", 0.8), ("Santo Domingo", 0.85, "Norte", 0.9),
            ("Fomerrey 106", 0.75, "Norte", 0.8), ("Cuauhtémoc", 0.80, "Centro", 0.8),
            ("Del Maestro", 1.00, "Norte", 0.8), ("Residencial Universidad", 1.05, "Norte", 0.9),
            ("Villas de San Nicolás", 0.78, "Norte", 0.7), ("Valle de Anáhuac", 0.95, "Norte", 0.7),
            ("Colonia Los Lermas", 0.82, "Norte", 0.6), ("San Nicolás Centro", 0.80, "Centro", 0.7),
        ]},
    "Apodaca": {
        "base_ppm2": 17500, "peso": 0.12,
        "aliases": ["Apodaca, N.L.", "Cd. Apodaca", "Apodaca N.L."],
        "colonias": [
            ("Apodaca Centro", 0.85, "Centro", 0.8), ("Huinalá", 0.80, "Oriente", 1.0),
            ("Nexxus", 1.15, "Poniente", 0.8), ("Hacienda Los Encinos", 1.00, "Poniente", 0.9),
            ("Puerta del Norte", 0.90, "Norte", 0.9), ("Vía Montaña", 1.05, "Poniente", 0.8),
            ("Villas de la Hacienda", 0.88, "Poniente", 0.9), ("Privadas de Apodaca", 0.82, "Centro", 0.8),
            ("Valle de las Palmas", 0.95, "Poniente", 0.7), ("Rincón de las Américas", 0.78, "Oriente", 0.7),
            ("Colonia Santa Rosa", 0.86, "Centro", 0.6), ("Paseo de Apodaca", 0.92, "Poniente", 0.6),
        ]},
    "General Escobedo": {
        "base_ppm2": 17000, "peso": 0.13,
        "aliases": ["Escobedo", "Gral. Escobedo", "General Escobedo, N.L.", "Escobedo, N.L."],
        "colonias": [
            ("Escobedo Centro", 0.85, "Centro", 0.8), ("Valle Soleado", 0.95, "Poniente", 1.0),
            ("Hacienda del Topo", 0.95, "Poniente", 0.9), ("Villas de San Francisco", 0.80, "Norte", 0.9),
            ("Sendero", 1.05, "Poniente", 0.9), ("Montesori", 0.85, "Norte", 0.8),
            ("Los Sauces", 0.90, "Norte", 0.8), ("Real de Palmas", 0.92, "Poniente", 0.8),
            ("Paseo de los Naranjos", 0.98, "Poniente", 0.7), ("Portal de Escobedo", 0.82, "Norte", 0.7),
            ("Colonia Nogales", 0.88, "Norte", 0.6), ("Misión de Escobedo", 0.93, "Poniente", 0.6),
        ]},
    "Santa Catarina": {
        "base_ppm2": 20000, "peso": 0.08,
        "aliases": ["Sta. Catarina", "Santa Catarina, N.L.", "Santa Catarina N.L."],
        "colonias": [
            ("Santa Catarina Centro", 0.85, "Centro", 0.8), ("Las Misiones", 1.05, "Poniente", 0.9),
            ("Fuentes de Santa Catarina", 0.90, "Poniente", 0.9), ("Paseo del Prado", 1.00, "Poniente", 0.8),
            ("Valle de Santa Catarina", 0.95, "Poniente", 0.9), ("Bosques de la Huasteca", 1.10, "Poniente", 0.6),
            ("Colonia La Fama", 0.78, "Centro", 0.7), ("Residencial Santa Catarina", 1.00, "Poniente", 0.7),
            ("Cerradas del Poniente", 0.92, "Poniente", 0.7), ("Los Cristales", 0.84, "Centro", 0.6),
        ]},
    "García": {
        "base_ppm2": 14500, "peso": 0.09,
        "aliases": ["Garcia", "García, N.L.", "Garcia N.L.", "Cd. García"],
        "colonias": [
            ("García Centro", 0.85, "Centro", 0.8), ("Valle de Lincoln", 1.00, "Norte", 1.0),
            ("Vistas del Río", 1.05, "Norte", 0.8), ("Paseo de las Villas", 0.92, "Norte", 0.9),
            ("Hacienda Mitras", 0.88, "Norte", 0.8), ("Real de Cumbres García", 0.95, "Norte", 0.8),
            ("Colinas de García", 0.90, "Norte", 0.7), ("Villa Mitras", 0.80, "Centro", 0.7),
            ("Misión del Valle", 0.98, "Norte", 0.6), ("Lomas de García", 0.86, "Centro", 0.6),
        ]},
    "Juárez": {
        "base_ppm2": 13500, "peso": 0.07,
        "aliases": ["Juarez", "Juárez, N.L.", "Cd. Juárez, N.L.", "Ciudad Benito Juárez"],
        "colonias": [
            ("Juárez Centro", 0.85, "Centro", 0.8), ("Paseo de los Naranjos Juárez", 1.00, "Sur", 0.9),
            ("Valle de Juárez", 0.95, "Sur", 0.9), ("Santa Rosa Juárez", 0.90, "Sur", 0.8),
            ("Colinas del Sur", 0.88, "Sur", 0.8), ("Hacienda San Pedro", 1.05, "Sur", 0.7),
            ("Residencial Huinalá Sur", 0.82, "Sur", 0.7), ("Villas del Sur", 0.80, "Sur", 0.7),
            ("Puerta de Hierro Juárez", 1.08, "Sur", 0.5), ("Colonia Benito Juárez", 0.78, "Centro", 0.6),
        ]},
    "Santiago": {
        "base_ppm2": 26000, "peso": 0.02,
        "aliases": ["Santiago, N.L.", "Santiago N.L.", "Villa de Santiago"],
        "colonias": [
            ("Santiago Centro", 0.90, "Centro", 1.0), ("El Cercado", 0.85, "Sur", 0.8),
            ("Cola de Caballo", 1.20, "Sierra", 0.8), ("Residencial Los Álamos", 1.00, "Centro", 0.8),
            ("Valle de Santiago", 0.95, "Sur", 0.7), ("Lomas de Santiago", 1.10, "Sierra", 0.6),
        ]},
}


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #
def crear_spark(nombre="CasaMTY", memoria="4g"):
    """SparkSession en modo local (sin clúster), tal como se definió en la Fase 2."""
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[*]").appName(nombre)
             .config("spark.driver.memory", memoria)
             .config("spark.sql.shuffle.partitions", "16")
             .config("spark.ui.enabled", "false")
             .config("spark.sql.session.timeZone", "America/Mexico_City")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def asegurar_carpetas():
    for p in (BRONCE, PLATA, ORO, METRICAS, WAREHOUSE_SQLITE.parent, POWERBI, DASHBOARD, DOCS, IMG):
        p.mkdir(parents=True, exist_ok=True)
