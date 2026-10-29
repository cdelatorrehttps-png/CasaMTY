# Diccionario de datos

> Todo el contenido es **simulado**. Venta y renta nunca se mezclan en un KPI de precio.

## Capa bronce (crudo) – `data/lake/bronce/<portal>/corrida=<fecha>/part-XXXX.jsonl.gz`

Un documento JSON por anuncio, tal como lo "publicó" cada portal. Los esquemas son distintos a propósito (Variedad):

| Portal | Campos principales |
|---|---|
| inmuebles24 | `id, titulo, precio` (texto: "$4.50 MDP", "USD 250,000", "$1,200,000 MXN"), `tipo_operacion, tipo_propiedad, m2_construccion, m2_terreno, recamaras, banos, estacionamientos, municipio, colonia, fecha_publicacion` |
| lamudi | `listing_id, title, price` (número), `currency, operation` (sale/rent), `property_type, built_area, land_area, bedrooms, bathrooms, parking, city, neighborhood, published_at` |
| vivanuncios | `aid, titulo, precio_txt` ("1,730,000"), `operacion` (venta/alquiler), `categoria, superficie` ("151 m²"), `habitaciones, banos, cochera, zona` ("Colonia, Municipio"), `fecha` |
| mercadolibre | `id, title, price{amount,currency_id}, listing_type, category, atributos{COVERED_AREA,TOTAL_AREA,BEDROOMS,FULL_BATHROOMS,PARKING_LOTS}, location{city,neighborhood}, date_created` |

## Capa plata (estandarizada) – `data/lake/plata/` (Parquet)

| Columna | Descripción |
|---|---|
| `id_anuncio_global` | Hash (fuente, id, fecha de captura) |
| `fuente`, `id_anuncio`, `fecha_captura`, `fecha_publicacion` | Origen y fechas |
| `operacion` | venta / renta |
| `tipo` | casa / departamento / terreno |
| `municipio`, `colonia`, `zona` | Normalizados contra el catálogo del AMM (nulos si no hay coincidencia) |
| `moneda`, `precio_mxn` | Precio convertido a pesos (USD × tipo de cambio fijo) |
| `m2`, `recamaras`, `banos`, `estacionamientos` | Atributos numéricos |
| `precio_m2` | `precio_mxn / m2` (solo si m² y precio son válidos) |
| `flag_municipio_valido`, `flag_colonia_valida`, `flag_m2_valido`, `flag_precio_valido` | Validaciones individuales |
| `flag_completo` | Municipio, colonia, m² y precio válidos |
| `flag_dup_recaptura` | Mismo portal e id en una corrida más reciente |
| `flag_dup_interportal` | Mismo inmueble en otro portal (zona + tipo + operación + m² + recámaras + precio) |
| `flag_duplicado` | Cualquiera de las dos anteriores |

## Capa oro / warehouse

| Tabla | Granularidad | Descripción |
|---|---|---|
| `hecho_anuncio` | 1 fila por anuncio capturado | Claves a las dimensiones, atributos, `precio_m2`, `flag_completo`, `flag_duplicado`, `tipo_duplicado`, `flag_outlier`, `flag_usable` |
| `hecho_estimacion` | colonia × tipo × operación | `n_anuncios_usados`, `precio_m2_mediana_observada`, `precio_m2_estimado_modelo`, `mape_zona`, `suficiente_muestra`, `cumple_meta_mape` |
| `dim_ubicacion` | colonia | municipio, colonia, zona; id 1 = "SIN MUNICIPIO / SIN COLONIA" |
| `dim_inmueble_tipo` | tipo | casa, departamento, terreno |
| `dim_operacion` | operación | venta, renta |
| `dim_fuente` | portal | nombre, tipo de extracción, fecha de última corrida |
| `agg_cubo_zona` | colonia × operación × tipo × rango m² × recámaras | `n`, mediana, p25, p75 de precio/m² y precio mediano (solo registros usables) |
| `agg_hist_precio_m2` | municipio × operación × tipo × intervalo | conteos para el histograma |
| `agg_tendencia` | corrida × municipio × operación × tipo | mediana de precio/m² por semana |
| `agg_calidad_fuente`, `agg_calidad_municipio` | portal / municipio | % completo, duplicado, atípico, usable |

`flag_usable` = completo **y** no duplicado **y** no atípico **y** no terreno. Es el filtro de todos los KPIs de precio.

Rangos de m² del cubo: `<60`, `60-89`, `90-119`, `120-179`, `180-249`, `250+` (índices 0–5). Recámaras: 0 = sin dato, 1–3, 4 = "4 o más".
