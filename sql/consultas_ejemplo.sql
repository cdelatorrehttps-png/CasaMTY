-- Consultas de ejemplo sobre el warehouse (SQLite o PostgreSQL). Sirven como origen para Power BI.
-- Recordatorio: en hecho_anuncio los flags son 1/0 en SQLite y true/false en PostgreSQL.

-- 1) Precio por m2 de casas en venta por municipio (solo anuncios usables)
SELECT u.municipio, COUNT(*) AS anuncios, ROUND(AVG(h.precio_m2)) AS ppm2_promedio
FROM hecho_anuncio h JOIN dim_ubicacion u USING (id_ubicacion)
WHERE h.flag_usable = 1 AND h.id_operacion = 1 AND h.id_tipo = 1
GROUP BY u.municipio ORDER BY ppm2_promedio DESC;

-- 2) Mezcla venta / renta por municipio (KPI "Mezcla compra/renta")
SELECT u.municipio, o.operacion, COUNT(*) AS anuncios
FROM hecho_anuncio h JOIN dim_ubicacion u USING (id_ubicacion) JOIN dim_operacion o USING (id_operacion)
WHERE h.flag_duplicado = 0
GROUP BY u.municipio, o.operacion;

-- 3) Cobertura geografica: colonias con al menos 30 anuncios validos (KPI "Cobertura")
SELECT COUNT(*) AS colonias_con_cobertura FROM (
  SELECT id_ubicacion FROM hecho_anuncio WHERE flag_usable = 1
  GROUP BY id_ubicacion HAVING COUNT(*) >= 30) t;

-- 4) Precio publicado vs estimado por colonia (KPI "Precio estimado vs precio publicado")
SELECT u.municipio, u.colonia, e.precio_m2_mediana_observada, e.precio_m2_estimado_modelo,
       ROUND(100.0 * (e.precio_m2_mediana_observada - e.precio_m2_estimado_modelo) / e.precio_m2_estimado_modelo, 1) AS desvio_pct,
       e.mape_zona, e.n_anuncios_usados
FROM hecho_estimacion e JOIN dim_ubicacion u USING (id_ubicacion)
WHERE e.id_operacion = 1 AND e.id_tipo = 1 AND e.suficiente_muestra = 1
ORDER BY u.municipio, u.colonia;

-- 5) Calidad de datos por portal (Veracidad)
SELECT f.fuente, COUNT(*) AS registros,
       ROUND(100.0 * AVG(h.flag_completo), 2)  AS pct_completo,
       ROUND(100.0 * AVG(h.flag_duplicado), 2) AS pct_duplicado,
       ROUND(100.0 * AVG(h.flag_outlier), 2)   AS pct_outlier
FROM hecho_anuncio h JOIN dim_fuente f USING (id_fuente)
GROUP BY f.fuente;

-- 6) Inventario semanal por operacion (tendencia)
SELECT h.fecha_captura, o.operacion, COUNT(*) AS anuncios_unicos
FROM hecho_anuncio h JOIN dim_operacion o USING (id_operacion)
WHERE h.flag_duplicado = 0
GROUP BY h.fecha_captura, o.operacion ORDER BY h.fecha_captura;
