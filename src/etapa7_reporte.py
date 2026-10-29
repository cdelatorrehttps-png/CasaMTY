"""
ETAPA 7 - REPORTE TÉCNICO (Fase 3)

Arma docs/Reporte_Tecnico_Fase3.md con las métricas REALES de la última corrida
(data/metricas/*.json) y lo convierte a .docx con pandoc si está instalado.
Así el reporte nunca queda desfasado respecto al código.
"""
import json
import shutil
import subprocess
from datetime import date

import config as C


def j(nombre):
    ruta = C.METRICAS / nombre
    return json.loads(ruta.read_text()) if ruta.exists() else {}


def n(x):
    return f"{x:,}" if isinstance(x, (int, float)) else str(x)


def main():
    ing, cal, oro = j("etapa1_ingesta.json"), j("etapa2_calidad.json"), j("etapa3_oro.json")
    mod, wh, tt = j("etapa4_modelo.json"), j("etapa5_warehouse.json"), j("tiempos_etapas.json")
    res = mod.get("resultados", {})
    v, r = res.get("venta", {}), res.get("renta", {})
    seg = sum(tt.get(k, 0) for k in ("etapa1", "etapa2", "etapa3", "etapa4", "etapa5"))
    imp = mod.get("importancia_permutacion", {}).get("venta", {})
    imp_txt = ", ".join(f"{k} ({val})" for k, val in sorted(imp.items(), key=lambda x: -x[1])[:4])

    filas_fuente = "\n".join(
        f"| {f['fuente']} | {n(f['n'])} | {f['pct_completo']}% | {f['pct_m2_valido']}% | {f['pct_colonia_valida']}% | {f['pct_duplicado']}% |"
        for f in cal.get("por_fuente", []))
    filas_tiempo = "\n".join(f"| {k.replace('etapa', 'Etapa ')} | {val} s |" for k, val in tt.items())
    filas_cons = "\n".join(f"| {k} | {val} s |" for k, val in wh.get("segundos_consultas", {}).items())

    def met(d, k):
        return f"{d[k]['mape_pct']}% / {d[k]['mae_mxn']:,.0f} / {d[k]['pct_dentro_20']}%"

    md = f"""---
title: "CasaMTY – Reporte técnico de la Fase 3 (Desarrollo / MVP)"
subtitle: "Proyecto integrador de tecnologías de Big Data"
author: "Cesar Leonardo De la Torre Villalobos · Matrícula 3061875 · Universidad Tecmilenio"
date: "{date.today().strftime('%d/%m/%Y')}"
---

> **Aviso importante.** El dataset de esta fase es **simulado** (la guía de la Fase 3 permite dataset simulado o real y la Fase 2 ya lo
> había previsto como riesgo controlado). Las cifras de precio y de error describen el comportamiento del *pipeline* y del modelo sobre
> datos sintéticos; **no son estadísticas del mercado real** del AMM. El tablero siempre habla de *precio aproximado de listado*, nunca de avalúo.

# 1. Resumen ejecutivo

CasaMTY es un prototipo funcional que recibe anuncios de vivienda de cuatro portales con formatos distintos, los limpia y unifica con
**Apache Spark en modo local**, los guarda en un **Data Lake por capas** (bronce / plata / oro), los modela en un **warehouse tipo estrella**,
estima el precio de listado con un modelo simple y lo muestra en un **dashboard** con filtros por municipio, colonia, operación, tipo, rango de m² y recámaras.

| Indicador | Resultado de la corrida |
|---|---|
| Registros crudos procesados | **{n(cal.get('registros_crudos'))}** (meta ≥ 1,000,000) |
| Registros únicos tras quitar duplicados | {n(cal.get('registros_unicos'))} |
| Registros usables para precio (completos, únicos, sin atípicos) | {n(oro.get('filas_usables'))} |
| Completitud de campos clave | **{cal.get('pct_completo')}%** (meta > 70%) |
| Tasa de duplicados detectados | {cal.get('tasa_duplicados_pct')}% |
| MAPE del estimado (venta / renta) | **{v.get('modelo', {}).get('mape_pct')}% / {r.get('modelo', {}).get('mape_pct')}%** (meta < 20%) |
| Reducción de error vs. "a ojo" (venta / renta) | {v.get('reduccion_error_vs_a_ojo_pct')}% / {r.get('reduccion_error_vs_a_ojo_pct')}% |
| Tiempo del pipeline (ingesta simulada + Spark + modelo + warehouse) | **{seg / 60:.1f} min** (meta < 30 min) |
| Municipios / colonias con datos | {oro.get('municipios_con_datos')} / {oro.get('colonias_con_datos')} |
| Costo de licencias | $0 |

# 2. Alcance del MVP y cumplimiento de la guía (sección 3.1)

| Requisito de la guía | Cómo se cumple | Dónde está |
|---|---|---|
| Dataset ≥ 1 millón de registros (simulado o real) | {n(cal.get('registros_crudos'))} registros crudos en 4 portales y 3 corridas semanales | `src/etapa1_ingesta_bronce.py`, `data/lake/bronce/` |
| Limpieza y transformación en Python | Generación, defectos controlados, normalización de texto, modelo y reporte en Python | `src/` |
| Procesamiento distribuido en Apache Spark | PySpark (`local[*]`): unión de esquemas, parseo de precios, catálogos *broadcast*, ventanas para duplicados, agregados | `src/etapa2_plata_spark.py`, `src/etapa3_oro_spark.py` |
| Almacenamiento estructurado | Parquet por capas + warehouse estrella (SQLite incluido; PostgreSQL opcional) | `data/lake/`, `src/etapa5_warehouse.py`, `sql/` |
| Dashboard funcional | `dashboard/index.html` (se abre sin instalar nada) y `powerbi/` + guía para Power BI Desktop | `dashboard/`, `powerbi/`, `docs/guia_powerbi.md` |

Lo que **no** se hizo y por qué: no se instaló MongoDB (la Fase 2 lo preveía para los documentos crudos; en el MVP los documentos crudos viven como JSONL
en la capa bronce y hay un script opcional `src/opcional_mongodb.py` para cargarlos); no se usó Kafka ni Airflow (decisión de la Fase 2: batch diario, orquestador en script);
y no se hizo *scraping* real de portales. Todo eso queda como mejora de la Fase 4.

# 3. Arquitectura implementada

```
 4 portales (esquemas distintos)        Python (batch)                 Spark local
 inmuebles24 · lamudi · vivanuncios ──►  etapa 1: ingesta  ──►  BRONCE  ──►  etapa 2: limpieza  ──►  PLATA
 mercadolibre   (JSONL.gz por corrida)   (dataset simulado)    (crudo)        (unifica, normaliza,    (Parquet)
                                                                               duplicados, calidad)
                                                                                        │
   Dashboard HTML  ◄── etapa 6 ◄── ORO ◄── etapa 3 (modelo estrella + agregados)  ◄──────┘
   Power BI (CSV)  ◄──────────────  │
                                     ├── etapa 4: modelo de estimación  ──►  hecho_estimacion
                                     └── etapa 5: warehouse (SQLite / PostgreSQL)
```

Orquestación: `correr_pipeline.py` ejecuta las 7 etapas en orden, mide el tiempo de cada una y se detiene si alguna falla
(equivalente escolar de Airflow, como se definió en la Fase 2). Se puede programar con el Programador de tareas de Windows o con `cron`.

# 4. Dataset (simulado) y las 5V

* **Volumen:** {n(cal.get('registros_crudos'))} registros crudos (~{n(ing.get('inmuebles_unicos_simulados'))} inmuebles únicos). Los 4 portales aportan 35 % / 25 % / 20 % / 20 %.
* **Velocidad:** 3 corridas semanales; un anuncio puede reaparecer en la corrida siguiente (recaptura) con 3 % de cambio de precio. El pipeline completo se repite con un solo comando.
* **Variedad:** cada portal usa nombres de campo, formatos de precio y anidación distintos (texto "$4.50 MDP", "USD 250,000", número plano, objeto `price.amount`, "151 m²", `atributos.COVERED_AREA`, "Colonia, Municipio" en un solo campo).
* **Veracidad:** se inyectan defectos controlados para poder medir calidad: m² faltantes (7 %) o inflados (3 %), precio faltante, renta capturada como venta, colonias con typos / sin acentos / en mayúsculas / con "Col.", municipio con alias ("SPGG", "Sta. Catarina"), duplicados entre portales y recapturas.
* **Valor:** estimar el precio aproximado de listado de una zona y comparar contra el precio publicado.

Los municipios y colonias del catálogo son reales (AMM), pero los **factores de precio por colonia son supuestos de diseño**, no cifras oficiales.

# 5. Resultados de la limpieza (Spark)

![Embudo de limpieza](img/02_embudo_limpieza.png){{width=85%}}

| Portal | Registros | % completo | % m² válido | % colonia válida | % duplicado |
|---|---|---|---|---|---|
{filas_fuente}
| **Total** | **{n(cal.get('registros_crudos'))}** | **{cal.get('pct_completo')}%** | **{cal.get('pct_m2_valido')}%** | **{cal.get('pct_colonia_valida')}%** | **{cal.get('tasa_duplicados_pct')}%** |

* Duplicados por recaptura (mismo portal e id, corrida más vieja): {n(cal.get('duplicados_recaptura'))}.
* Duplicados entre portales (misma zona, tipo, operación, m², recámaras y precio): {n(cal.get('duplicados_interportal'))}.
* Anuncios en USD convertidos a MXN con tipo de cambio fijo de {C.FX_USD_MXN}: {n(cal.get('anuncios_en_usd'))}.
* Regla de atípicos: se descarta un registro cuyo precio por m² quede fuera de [mediana/{C.FACTOR_OUTLIER:.0f}, mediana×{C.FACTOR_OUTLIER:.0f}] de su municipio, operación y tipo (esto atrapa, por ejemplo, rentas capturadas como venta).
* Venta y renta **nunca** se mezclan en un mismo KPI de precio.

![Calidad por portal](img/05_calidad_portal.png){{width=75%}}

# 6. Modelo de estimación (sección 3.2: indicadores)

Pregunta de la Fase 1: ¿se puede estimar el precio de listado con un error aceptable usando solo atributos publicados?

Se entrenó un `HistGradientBoostingRegressor` (scikit-learn) por operación, con variables: municipio, colonia, tipo, m², recámaras, baños y estacionamientos.
Se evalúa en el 20 % de los datos que **no** se usó para entrenar y se compara contra dos métodos tradicionales.

| Método | Venta: MAPE / MAE (MXN) / % dentro de ±20 % | Renta: MAPE / MAE (MXN) / % dentro de ±20 % |
|---|---|---|
| A ojo (promedio del municipio) | {met(v, 'a_ojo_promedio_municipio')} | {met(r, 'a_ojo_promedio_municipio')} |
| Mediana por m² del municipio × m² | {met(v, 'mediana_m2_municipio')} | {met(r, 'mediana_m2_municipio')} |
| **Modelo CasaMTY** | **{met(v, 'modelo')}** | **{met(r, 'modelo')}** |

* Reducción de error del modelo frente al método "a ojo": **{v.get('reduccion_error_vs_a_ojo_pct')} %** en venta y **{r.get('reduccion_error_vs_a_ojo_pct')} %** en renta.
* Variables más influyentes en venta (importancia por permutación): {imp_txt}.
* Zonas (colonia × tipo × operación) con ≥ {C.UMBRAL_MUESTRA_ZONA} anuncios: {mod.get('zonas_con_suficiente_muestra')} de {mod.get('zonas_totales')}; de ellas, **{mod.get('pct_zonas_suficientes_que_cumplen_meta')} %** cumplen MAPE < {C.META_MAPE:.0f} %.
* Tiempo de entrenamiento total: {mod.get('segundos_entrenamiento_total')} s.

![MAPE comparativo](img/03_mape_comparativo.png){{width=80%}}

![Real vs estimado](img/04_real_vs_estimado.png){{width=80%}}

**Cómo leer estos números con honestidad.** Como los datos son sintéticos y el precio se generó con una fórmula que depende de zona, tamaño y recámaras (más ruido),
es *esperable* que un modelo que usa esas mismas variables lo recupere bien. El resultado demuestra que el pipeline y el modelo funcionan de punta a punta
y que el diseño de variables es coherente; **no demuestra** que el mercado real se pueda estimar con ese error. Con datos reales, el MAPE sería mayor
(estado físico, vista, negociación, antigüedad, etc.), y esa validación queda como trabajo de la Fase 4.

# 7. Indicadores técnicos y de negocio (métricas obtenidas)

| Indicador | Meta (Fase 2) | Resultado |
|---|---|---|
| Tiempo promedio de procesamiento (pipeline) | < 30 min | {seg / 60:.1f} min |
| Completitud de campos clave | > 70 % | {cal.get('pct_completo')} % |
| Tasa de duplicados | registrar y bajar | {cal.get('tasa_duplicados_pct')} % detectados y marcados |
| MAPE del estimado | < 20 % | venta {v.get('modelo', {}).get('mape_pct')} % · renta {r.get('modelo', {}).get('mape_pct')} % |
| Reducción de error vs. método tradicional | informativo | {v.get('reduccion_error_vs_a_ojo_pct')} % venta · {r.get('reduccion_error_vs_a_ojo_pct')} % renta |
| Cobertura AMM | ≥ 8 municipios | {oro.get('municipios_con_datos')} municipios · {oro.get('colonias_con_datos')} colonias |
| Costo de licencias | $0 | $0 |
| Costo por consulta analítica | informativo | {wh.get('segundos_promedio_por_consulta')} s promedio, $0 (SQLite local) |
| Valor de uso del tablero | ≤ 3 clics | 2 selecciones (municipio → colonia) |

Tiempos por etapa (hardware de la corrida: 2 núcleos, 7 GB de RAM):

| Etapa | Tiempo |
|---|---|
{filas_tiempo}

Consultas analíticas típicas sobre el warehouse ({n(wh.get('filas_hecho_anuncio'))} filas en `hecho_anuncio`, tamaño del archivo {wh.get('tamano_sqlite_mb')} MB):

| Consulta | Mejor de 3 corridas |
|---|---|
{filas_cons}

La etapa de ingesta incluye la **generación** del dataset simulado; con datos reales ese tiempo sería de descarga y dependería de los portales.

# 8. Modelo de datos del warehouse

Esquema estrella definido en la Fase 2: `hecho_anuncio` y `hecho_estimacion` como hechos; `dim_ubicacion`, `dim_inmueble_tipo`, `dim_operacion` y `dim_fuente` como dimensiones.
El DDL para PostgreSQL está en `sql/esquema_postgresql.sql`; las consultas de ejemplo para Power BI, en `sql/consultas_ejemplo.sql`.

# 9. Dashboard operativo

`dashboard/index.html` incluye: filtros (municipio, colonia, operación, tipo, rango de m², recámaras), tarjetas KPI (precio mediano por m², inventario, mezcla venta/renta,
cobertura, MAPE), precio por municipio, colonias más caras y más baratas, histograma, tendencia semanal, estructura de la oferta, calidad por portal, embudo de limpieza,
tabla de cumplimiento de metas, comparación del modelo y un **valuador de orientación** (captura colonia, tipo, m² y recámaras y entrega rango y nivel de confianza).
Las capturas están en `docs/img/dashboard_*.png`. Para la versión Power BI Desktop (la que nombra la guía), ver `docs/guia_powerbi.md`.

![Dashboard](img/dashboard_01_mercado.png){{width=100%}}

# 10. Propuesta de mejora organizacional (sección 3.3)

Ver `docs/propuesta_organizacional.md` (roles, procesos, gobierno de datos, seguridad y cumplimiento). Resumen:

* **Roles:** Data Engineer (pipeline y lake), Data Analyst (tablero y KPIs), responsable de calidad de datos (*data steward*), y un dueño del producto que decide qué se publica.
* **Procesos:** corrida semanal programada, revisión de calidad antes de publicar (completitud, duplicados, atípicos), bitácora de métricas por corrida y control de versiones del código.
* **Gobierno de datos:** catálogo y diccionario de datos, reglas de calidad versionadas, linaje bronce → plata → oro y etiqueta obligatoria de "simulado" o "precio de listado".
* **Seguridad y cumplimiento:** sin teléfonos, nombres de vendedores ni fotos; solo datos públicos de mercado; respetar términos de uso y `robots.txt` si algún día se hace *scraping*; aviso de privacidad si se usaran datos personales (LFPDPPP).

# 11. Limitaciones y trabajo futuro

1. **Datos simulados.** El siguiente paso es sustituir la etapa 1 por una ingesta real y ética (descargas públicas, API autorizada o *scraping* respetuoso) conservando el mismo esquema bronce.
2. **El modelo no está validado con mercado real.** Hay que reportar el MAPE con datos reales y por municipio.
3. **Warehouse local.** SQLite cumple para el MVP; la Fase 2 definió PostgreSQL (el script ya acepta una URL de SQLAlchemy).
4. **Sin MongoDB, Kafka ni Airflow** en el MVP, por la decisión de escala de la Fase 2.
5. **Detección de duplicados entre portales** es por coincidencia exacta de zona, m², recámaras y precio; con datos reales conviene una comparación aproximada (*fuzzy*).
6. **Tipo de cambio fijo** para anuncios en USD.
7. **Tendencia semanal** con solo 3 corridas; no hay series largas.

# 12. Cómo reproducir

```
pip install -r requirements.txt
python correr_pipeline.py            # 1.5 millones de registros
python correr_pipeline.py --n 100000 # prueba rápida
```

Requiere Python 3.10+ y Java 11/17/21 (para Spark). Detalles en `README.md`.
"""
    C.DOCS.mkdir(parents=True, exist_ok=True)
    ruta_md = C.DOCS / "Reporte_Tecnico_Fase3.md"
    ruta_md.write_text(md, encoding="utf-8")
    print(f"[reporte] {ruta_md}")
    if shutil.which("pandoc"):
        out = C.DOCS / "Reporte_Tecnico_Fase3.docx"
        p = subprocess.run(["pandoc", str(ruta_md), "-o", str(out), "--resource-path", str(C.DOCS)], capture_output=True, text=True)
        print("[reporte] docx:", out if p.returncode == 0 else "falló pandoc: " + p.stderr[:200])
    else:
        print("[reporte] pandoc no está instalado; solo se generó el .md")


if __name__ == "__main__":
    main()
