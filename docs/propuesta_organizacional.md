# Propuesta de mejora organizacional (Fase 3, sección 3.3)

Contexto: CasaMTY empieza como proyecto escolar de una persona, pero se plantea como si fuera a operar dentro de una inmobiliaria pequeña
o un despacho de asesoría de vivienda en el AMM. Esta propuesta responde a los cuatro puntos que pide la guía.

## 1. Nuevos roles

| Rol | Responsabilidad | En el proyecto escolar |
|---|---|---|
| **Data Engineer** | Construir y mantener el pipeline (ingesta, Spark, lake, warehouse), programar las corridas, vigilar tiempos y fallos | Etapas 1–5 y `correr_pipeline.py` |
| **Data Analyst** | Definir KPIs, construir y mantener el tablero (Power BI), interpretar resultados con el negocio | Etapa 6, `docs/guia_powerbi.md` |
| **Data Steward (calidad de datos)** | Dueño de las reglas de calidad: qué es un m² válido, cómo se deduplica, qué es un atípico; aprueba cambios a esas reglas | `src/config.py` y banderas `flag_*` |
| **Dueño del producto (negocio)** | Decide qué se muestra a los asesores y con qué leyenda ("precio aproximado de listado") | Docente / usuario final |

En una organización pequeña una misma persona puede ocupar varios roles; lo importante es que cada responsabilidad tenga un nombre asignado.

## 2. Nuevos procesos

1. **Corrida semanal programada** (`correr_pipeline.py` con tarea programada). Hoy los comparativos se arman "a ojo" en Excel (≈ 4 horas por colonia según la Fase 2); el pipeline lo resuelve en minutos.
2. **Revisión de calidad antes de publicar.** Antes de abrir el tablero se revisan: % de completitud (> 70 %), tasa de duplicados, registros atípicos y cobertura de colonias. Si una métrica cae, la corrida se marca como "no publicable".
3. **Bitácora de métricas por corrida** (`data/metricas/*.json`), para comparar semana contra semana y justificar cambios.
4. **Control de versiones** del código y de las reglas de negocio (Git), con cambios revisados por otra persona.
5. **Reglas de publicación:** siempre mostrar la etiqueta de datos (simulado / real) y "precio de listado, no avalúo".

## 3. Gobierno de datos

* **Catálogo y diccionario de datos:** cada tabla de la capa oro tiene definición, dueño y frecuencia de actualización (ver tabla abajo y `docs/diccionario_datos.md`).
* **Linaje:** bronce (crudo, inmutable) → plata (estandarizado, con banderas de calidad) → oro (modelo de negocio). Nada se corrige en bronce; toda corrección es una regla en plata.
* **Reglas de calidad versionadas** en `src/config.py` y en `etapa2_plata_spark.py` (rangos de m², factor de atípicos, umbral de muestra).
* **Regla de negocio central:** venta y renta nunca se mezclan en un mismo KPI de precio.
* **Transparencia de la suciedad:** los registros incompletos y duplicados no se borran; se marcan, y el tablero muestra su porcentaje.
* **Trazabilidad del estimado:** cada estimación guarda `n_anuncios_usados`, `mape_zona` y `fecha_modelo`; no se da estimación si no hay muestra suficiente (`suficiente_muestra`).

| Dato | Dueño | Capa | Actualización |
|---|---|---|---|
| Anuncios crudos | Data Engineer | bronce | cada corrida |
| Anuncios estandarizados y banderas | Data Steward | plata | cada corrida |
| Hechos, dimensiones y agregados | Data Analyst | oro / warehouse | cada corrida |
| Estimaciones por zona | Data Analyst | oro | cada corrida |

## 4. Seguridad y cumplimiento

* **Minimización de datos personales:** el esquema **no** guarda teléfonos, nombres de vendedores, correos, URLs ni fotos; el análisis es de mercado (precio, zona, características).
* **Uso ético de fuentes reales:** solo información pública de listados, respetar términos de uso y `robots.txt` de cada portal, pausas entre descargas, sin cuentas personales.
* **Marco legal de referencia (México):** si en el futuro se almacenaran datos personales, aplicaría la Ley Federal de Protección de Datos Personales en Posesión de los Particulares (LFPDPPP) y se requeriría aviso de privacidad. Consultar al área jurídica de la institución antes de usar datos reales a escala.
* **Control de accesos:** el lake y el warehouse son locales; en una organización real, usuarios con permisos de solo lectura para analistas y de escritura solo para el Data Engineer, y credenciales fuera del código (variables de entorno).
* **Respaldo y recuperación:** el crudo en bronce es la fuente de verdad; plata y oro se pueden regenerar con un comando.
* **Uso responsable de la estimación:** no es avalúo ni sirve para trámite notarial o crédito; el tablero lo dice en un aviso fijo.
