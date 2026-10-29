# CasaMTY – Fase 3 (Desarrollo / MVP)

Proyecto integrador de tecnologías de Big Data · Cesar Leonardo De la Torre Villalobos · Tecmilenio

Prototipo funcional de la idea definida en las Fases 1 y 2: **estimar el precio aproximado de listado de vivienda en el Área Metropolitana de Monterrey**
a partir de anuncios de varios portales, con un pipeline batch en Python + Apache Spark local, un Data Lake por capas, un warehouse estrella y un dashboard.

> ⚠️ **Los datos son SIMULADOS** (la guía de la Fase 3 permite dataset simulado o real). Los municipios y colonias son reales, pero los precios y su relación con la zona
> son supuestos de diseño. Las métricas demuestran que el pipeline funciona, no describen el mercado real.

## Qué contiene

| Entregable de la guía (Fase 3) | Dónde está |
|---|---|
| Código documentado | `src/` (cada script tiene su encabezado explicativo), `correr_pipeline.py` |
| Dashboard operativo | `dashboard/index.html` (abrir con el navegador) · `powerbi/` + `docs/guia_powerbi.md` para Power BI Desktop |
| Reporte técnico | `docs/Reporte_Tecnico_Fase3.docx` (y `.md`) |
| Métricas obtenidas | `data/metricas/*.json` y el reporte |
| Propuesta de mejora organizacional (3.3) | `docs/propuesta_organizacional.md` |
| Diccionario de datos | `docs/diccionario_datos.md` |

## Cómo verlo (sin instalar nada)

Abre `dashboard/index.html` en el navegador (Chrome, Edge o Firefox). Incluye filtros, KPIs, gráficas, calidad de datos, tabla de metas y un valuador de orientación.

## Cómo correr el pipeline completo

Requisitos: Python 3.10+ y Java 11, 17 o 21 (Spark lo necesita). Memoria recomendada: 8 GB.

```bash
pip install -r requirements.txt
python correr_pipeline.py                 # 1.5 millones de registros crudos (~minutos)
python correr_pipeline.py --n 100000      # prueba rápida
python correr_pipeline.py --desde 3       # reanuda desde la etapa 3
```

En Windows, si Spark se queja de `winutils` / `HADOOP_HOME`, es un aviso conocido de Hadoop en Windows; para el modo local con Parquet normalmente funciona,
y si no, ejecuta el proyecto en WSL o en Google Colab (la Fase 2 ya lo contemplaba como opción).

## Etapas del pipeline

| # | Script | Qué hace | Salida |
|---|---|---|---|
| 1 | `etapa1_ingesta_bronce.py` | Genera el dataset simulado (4 portales con esquemas distintos, 3 corridas semanales, defectos controlados) | `data/lake/bronce/` (JSONL.gz) |
| 2 | `etapa2_plata_spark.py` | **Spark**: unifica esquemas, parsea precios, normaliza municipio/colonia, detecta duplicados, marca calidad | `data/lake/plata/` (Parquet) |
| 3 | `etapa3_oro_spark.py` | **Spark**: modelo estrella, atípicos, cubo y agregados para el tablero | `data/lake/oro/`, `powerbi/*.csv` |
| 4 | `etapa4_modelo.py` | Modelo de estimación (gradient boosting) vs. métodos tradicionales; MAPE/MAE | `hecho_estimacion`, `etapa4_modelo.json` |
| 5 | `etapa5_warehouse.py` | Carga el warehouse (SQLite; PostgreSQL con `--url`) y mide consultas | `data/warehouse/casamty.sqlite` |
| 6 | `etapa6_dashboard.py` | Genera `dashboard/index.html` y las gráficas del reporte | `dashboard/`, `docs/img/` |
| 7 | `etapa7_reporte.py` | Redacta el reporte técnico con las métricas reales | `docs/Reporte_Tecnico_Fase3.*` |

Opcionales: `src/captura_dashboard.py` (capturas de pantalla), `src/opcional_mongodb.py` (carga de crudos a MongoDB), `sql/esquema_postgresql.sql`.

## Decisiones de diseño (alineadas con la Fase 2)

* **Batch, no streaming:** los anuncios cambian por día/semana, no por segundo → sin Kafka.
* **Spark en modo local:** misma herramienta que pide la materia, sin clúster ni nube → $0.
* **Lake en carpetas + Parquet** como equivalente escolar de HDFS (bronce / plata / oro).
* **Orquestador = script** (`correr_pipeline.py`) programable con el Programador de tareas o `cron`; Airflow queda como mejora.
* **Warehouse:** SQLite por defecto para que cualquiera lo reproduzca; PostgreSQL (el definido en la Fase 2) con una URL.
* **Venta y renta nunca se mezclan** en un mismo KPI de precio.
* **Ética:** sin teléfonos, nombres de vendedores ni fotos; precio de listado ≠ avalúo.

## Estructura

```
CasaMTY_Fase3/
├── correr_pipeline.py        orquestador
├── requirements.txt
├── src/                      código de cada etapa
├── data/
│   ├── lake/{bronce,plata,oro}/   (se genera al correr; ver nota de tamaño abajo)
│   ├── warehouse/casamty.sqlite
│   └── metricas/*.json
├── dashboard/index.html
├── powerbi/*.csv
├── sql/
└── docs/
```

> **Nota de tamaño:** para que el zip sea ligero **no se incluyen** el crudo completo (`data/lake/bronce/`, 1.5 M de registros), la capa plata, la tabla de hechos
> en Parquet ni el archivo SQLite del warehouse (~150 MB). Todo se regenera con `python correr_pipeline.py` (≈ 4–5 minutos en una laptop de 2 núcleos).
> Sí se incluyen: los agregados y dimensiones de la capa oro, el dashboard, los CSV de Power BI (con una muestra de 50 mil anuncios), las métricas de la corrida real,
> una muestra de 200 documentos crudos por portal en `data/muestra/` (para enseñar la Variedad de esquemas) y la documentación.
