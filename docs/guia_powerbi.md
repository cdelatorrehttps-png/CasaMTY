# Guía para armar el tablero en Power BI Desktop

La guía del proyecto integrador nombra **Power BI** como herramienta de visualización. Este proyecto incluye dos versiones del tablero:

1. `dashboard/index.html`: versión lista para mostrar (se abre en el navegador, sin instalar nada).
2. Esta guía: cómo armar la misma vista en Power BI Desktop (gratuito, solo Windows) con los archivos de la carpeta `powerbi/`.

> Importante: Power BI Desktop no se puede ejecutar desde el entorno donde se generó este proyecto, así que el archivo `.pbix` **no** viene incluido.
> Los CSV están listos para importarse y las medidas DAX de abajo son las del tablero HTML. Armar el `.pbix` toma unos 20–30 minutos.

## 1. Cargar los datos

*Obtener datos → Texto/CSV* y cargar, de la carpeta `powerbi/`:

| Archivo | Para qué |
|---|---|
| `dim_ubicacion.csv`, `dim_inmueble_tipo.csv`, `dim_operacion.csv`, `dim_fuente.csv` | Dimensiones |
| `hecho_estimacion.csv` | Estimado por colonia × tipo × operación (mapa/tabla del precio aproximado de la zona) |
| `agg_cubo_zona.csv` | Cubo con n, mediana y cuartiles de precio por m² (colonia × operación × tipo × rango de m² × recámaras) |
| `agg_tendencia.csv`, `agg_hist_precio_m2.csv` | Tendencia semanal e histograma |
| `agg_calidad_fuente.csv`, `agg_calidad_municipio.csv` | Calidad de datos |
| `hecho_anuncio_muestra_50k.csv` | Muestra de 50,000 anuncios usables para explorar a nivel anuncio |

Alternativa con PostgreSQL (como en la Fase 2): *Obtener datos → Base de datos PostgreSQL*, después de correr
`python src/etapa5_warehouse.py --url postgresql+psycopg2://usuario:clave@localhost:5432/casamty`.

## 2. Modelo (vista de relaciones)

* `hecho_estimacion[id_ubicacion]` → `dim_ubicacion[id_ubicacion]` (muchos a uno)
* `hecho_estimacion[id_tipo]` → `dim_inmueble_tipo[id_tipo]`
* `hecho_estimacion[id_operacion]` → `dim_operacion[id_operacion]`
* `hecho_anuncio_muestra_50k` con las mismas tres claves + `id_fuente`
* `agg_cubo_zona` se relaciona por texto con `dim_ubicacion` (columnas `municipio` y `colonia`) y con `dim_operacion[operacion]`, `dim_inmueble_tipo[tipo]`. Si Power BI pide una clave única, crea una columna `municipio|colonia` en ambas tablas.

Regla de negocio: **siempre** poner la segmentación `operacion` en la página o usar una medida por operación; venta y renta no se mezclan en un mismo número.

## 3. Medidas DAX sugeridas

```DAX
Anuncios usables = SUM ( agg_cubo_zona[n] )

Precio mediano m2 (aprox.) =
DIVIDE (
    SUMX ( agg_cubo_zona, agg_cubo_zona[ppm2_mediana] * agg_cubo_zona[n] ),
    [Anuncios usables]
)

Cobertura (colonias con >= 30 anuncios) =
COUNTROWS (
    FILTER (
        SUMMARIZE ( agg_cubo_zona, agg_cubo_zona[municipio], agg_cubo_zona[colonia] ),
        CALCULATE ( SUM ( agg_cubo_zona[n] ) ) >= 30
    )
)

Mezcla venta % =
DIVIDE (
    CALCULATE ( [Anuncios usables], agg_cubo_zona[operacion] = "venta" ),
    CALCULATE ( [Anuncios usables], ALL ( agg_cubo_zona[operacion] ) )
)

MAPE promedio de zonas con muestra =
CALCULATE ( AVERAGE ( hecho_estimacion[mape_zona] ), hecho_estimacion[suficiente_muestra] = TRUE () )

Desvio publicado vs estimado % =
DIVIDE (
    AVERAGE ( hecho_estimacion[precio_m2_mediana_observada] ) - AVERAGE ( hecho_estimacion[precio_m2_estimado_modelo] ),
    AVERAGE ( hecho_estimacion[precio_m2_estimado_modelo] )
)
```

Nota: la mediana ponderada de medianas es una aproximación; para la mediana exacta a nivel anuncio usa `hecho_anuncio_muestra_50k` o la consulta correspondiente en SQL.

## 4. Páginas sugeridas

**Página 1 – Mercado por zona** (equivale a la sección "Mercado" del HTML)
Segmentaciones: municipio, colonia, operación, tipo, rango de m² (`m2b`), recámaras (`recb`).
Tarjetas: Precio mediano m², Anuncios usables, Mezcla venta %, Cobertura.
Visuales: barras por municipio, barras de colonias (Top N 8 y Bottom N 8), histograma (`agg_hist_precio_m2`), líneas de tendencia (`agg_tendencia`).

**Página 2 – Calidad y metas**
Tabla de `agg_calidad_fuente`, barras de `agg_calidad_municipio`, tarjetas con completitud, duplicados y MAPE; tabla de metas (ver `data/metricas/*.json`).

**Página 3 – Estimación**
Tabla de `hecho_estimacion` con formato condicional en `desvio %` y `mape_zona`, dispersión real vs estimado con `hecho_anuncio_muestra_50k`.

Etiqueta fija en todas las páginas (cuadro de texto): *"Datos simulados con fines académicos. Precio aproximado de listado; no es avalúo."*

## 5. Métricas de uso

Meta de la Fase 2: el usuario responde "precio por m² de una colonia" en **menos de 3 clics** (municipio → colonia → lectura de la tarjeta). Verifícalo con un compañero y anota el resultado en el reporte.
