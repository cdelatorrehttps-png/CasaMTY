"""
ETAPA 3 - CAPA ORO: modelo dimensional (estrella) + agregados para dashboard / Power BI

Entrada : lake/plata/
Salida  : lake/oro/ (Parquet)
            dim_ubicacion, dim_inmueble_tipo, dim_operacion, dim_fuente
            hecho_anuncio                (1 fila por anuncio capturado, con banderas de calidad)
            agg_cubo_zona                (precio/m² por colonia x operación x tipo x rango m² x recámaras)
            agg_hist_precio_m2           (histograma de precio/m²)
            agg_calidad_fuente / agg_calidad_municipio / agg_tendencia
          powerbi/*.csv                  (dimensiones y agregados pequeños listos para Power BI)

Regla de negocio: venta y renta NUNCA se mezclan en un mismo KPI de precio.
"""
import json
import time

import pandas as pd
from pyspark.sql import functions as F

import config as C

TIPOS = ["casa", "departamento", "terreno"]
OPERACIONES = ["venta", "renta"]
STEP_HIST = {"venta": 5000, "renta": 25}   # ancho del bin de histograma (MXN/m²)
N_BINS = 24


def bucket_m2(col):
    expr = None
    for i, (lo, hi, _) in reversed(list(enumerate(C.M2_BUCKETS))):
        cond = (col >= lo) & (col < hi)
        expr = F.when(cond, F.lit(i)) if expr is None else F.when(cond, F.lit(i)).otherwise(expr)
    return expr


def main():
    t0 = time.time()
    C.asegurar_carpetas()
    spark = C.crear_spark("CasaMTY-oro")
    try:
        p = spark.read.parquet(str(C.PLATA))

        # ---------------- dimensiones ----------------
        filas = [(0, "SIN MUNICIPIO", "SIN COLONIA", "SIN ZONA")]
        for muni, info in C.MUNICIPIOS.items():
            filas.append((0, muni, "SIN COLONIA", "SIN ZONA"))
            for nom, _, zona, _ in info["colonias"]:
                filas.append((0, muni, nom, zona))
        dim_ub = pd.DataFrame(filas, columns=["id_ubicacion", "municipio", "colonia", "zona"])
        dim_ub["id_ubicacion"] = range(1, len(dim_ub) + 1)
        dim_tipo = pd.DataFrame({"id_tipo": [1, 2, 3], "tipo": TIPOS, "es_vivienda": [True, True, False]})
        dim_op = pd.DataFrame({"id_operacion": [1, 2], "operacion": OPERACIONES})
        ultima = {r["fuente"]: str(r["f"]) for r in p.groupBy("fuente").agg(F.max("fecha_captura").alias("f")).collect()}
        dim_fu = pd.DataFrame({"id_fuente": range(1, len(C.PORTALES) + 1), "fuente": list(C.PORTALES),
                               "tipo_extraccion": "simulado (JSONL batch)",
                               "fecha_ultima_corrida": [ultima.get(f) for f in C.PORTALES]})
        sd_ub, sd_tipo, sd_op, sd_fu = (spark.createDataFrame(x) for x in (dim_ub, dim_tipo, dim_op, dim_fu))

        # ---------------- hecho_anuncio ----------------
        p = (p.withColumn("municipio_k", F.coalesce("municipio", F.lit("SIN MUNICIPIO")))
              .withColumn("colonia_k", F.coalesce("colonia", F.lit("SIN COLONIA"))))
        # outliers: precio/m² fuera de [mediana/3, mediana*3] dentro de municipio x operación x tipo
        base_ok = F.col("flag_completo") & ~F.col("flag_duplicado") & (F.col("tipo") != "terreno")
        med = (p.where(base_ok).groupBy("municipio", "operacion", "tipo")
               .agg(F.percentile_approx("precio_m2", 0.5).alias("med_ppm2")))
        p = p.join(F.broadcast(med), ["municipio", "operacion", "tipo"], "left")
        p = p.withColumn("flag_outlier", F.coalesce(
            (F.col("precio_m2") < F.col("med_ppm2") / C.FACTOR_OUTLIER) | (F.col("precio_m2") > F.col("med_ppm2") * C.FACTOR_OUTLIER),
            F.lit(False)))
        p = p.withColumn("flag_usable", base_ok & ~F.col("flag_outlier"))
        p = p.withColumn("tipo_duplicado", F.when(F.col("flag_dup_recaptura"), "recaptura")
                         .when(F.col("flag_dup_interportal"), "interportal"))

        p = (p.join(F.broadcast(sd_ub.select("id_ubicacion", F.col("municipio").alias("municipio_k"), F.col("colonia").alias("colonia_k"))),
                    ["municipio_k", "colonia_k"], "left")
             .join(F.broadcast(sd_tipo.select("id_tipo", "tipo")), "tipo", "left")
             .join(F.broadcast(sd_op.select("id_operacion", "operacion")), "operacion", "left")
             .join(F.broadcast(sd_fu.select("id_fuente", "fuente")), "fuente", "left"))
        hecho = p.select("id_anuncio_global", "id_fuente", "id_ubicacion", "id_tipo", "id_operacion", "fecha_captura",
                         "fecha_publicacion", "moneda", "precio_mxn", "m2", "recamaras", "banos", "estacionamientos",
                         "precio_m2", "flag_completo", "flag_duplicado", "tipo_duplicado", "flag_outlier", "flag_usable")
        ruta_hecho = str(C.ORO / "hecho_anuncio")
        hecho.repartition(4).write.mode("overwrite").parquet(ruta_hecho)
        for nombre, d in [("dim_ubicacion", dim_ub), ("dim_inmueble_tipo", dim_tipo), ("dim_operacion", dim_op), ("dim_fuente", dim_fu)]:
            d.to_parquet(C.ORO / f"{nombre}.parquet", index=False)
            d.to_csv(C.POWERBI / f"{nombre}.csv", index=False, encoding="utf-8-sig")

        # ---------------- agregados (se leen del hecho ya escrito) ----------------
        h = spark.read.parquet(ruta_hecho)
        ub = spark.createDataFrame(dim_ub)
        u = h.where("flag_usable").join(F.broadcast(ub), "id_ubicacion") \
             .join(F.broadcast(sd_tipo.select("id_tipo", "tipo")), "id_tipo") \
             .join(F.broadcast(sd_op.select("id_operacion", "operacion")), "id_operacion")
        u = u.withColumn("m2b", bucket_m2(F.col("m2"))) \
             .withColumn("recb", F.when(F.col("recamaras").isNull(), 0).otherwise(F.least(F.greatest("recamaras", F.lit(1)), F.lit(4))))

        t_q = time.time()
        cubo = (u.groupBy("municipio", "colonia", "operacion", "tipo", "m2b", "recb").agg(
            F.count("*").alias("n"),
            F.percentile_approx("precio_m2", 0.5).alias("ppm2_mediana"),
            F.percentile_approx("precio_m2", 0.25).alias("ppm2_p25"),
            F.percentile_approx("precio_m2", 0.75).alias("ppm2_p75"),
            F.percentile_approx("precio_mxn", 0.5).alias("precio_mediano")).toPandas())
        seg_cubo = time.time() - t_q
        cubo.to_parquet(C.ORO / "agg_cubo_zona.parquet", index=False)

        step = F.when(F.col("operacion") == "venta", STEP_HIST["venta"]).otherwise(STEP_HIST["renta"])
        hist = (u.withColumn("bin", F.least(F.floor(F.col("precio_m2") / step), F.lit(N_BINS)).cast("int"))
                .groupBy("municipio", "operacion", "tipo", "bin").agg(F.count("*").alias("n")).toPandas())
        hist["step"] = hist["operacion"].map(STEP_HIST)
        hist.to_parquet(C.ORO / "agg_hist_precio_m2.parquet", index=False)

        tend = (u.groupBy("fecha_captura", "municipio", "operacion", "tipo").agg(
            F.count("*").alias("n"), F.percentile_approx("precio_m2", 0.5).alias("ppm2_mediana")).toPandas())
        tend["fecha_captura"] = tend["fecha_captura"].astype(str)
        tend.to_parquet(C.ORO / "agg_tendencia.parquet", index=False)

        # calidad por fuente y por municipio (todas las filas de plata)
        pct = lambda c: F.round(F.avg(F.col(c).cast("int")) * 100, 2)
        fu = sd_fu.select("id_fuente", "fuente")
        cal_f = (h.join(F.broadcast(fu), "id_fuente").groupBy("fuente").agg(
            F.count("*").alias("n"), pct("flag_completo").alias("pct_completo"),
            pct("flag_duplicado").alias("pct_duplicado"), pct("flag_outlier").alias("pct_outlier"),
            pct("flag_usable").alias("pct_usable")).toPandas())
        cal_m = (h.join(F.broadcast(ub), "id_ubicacion").groupBy("municipio").agg(
            F.count("*").alias("n"), pct("flag_completo").alias("pct_completo"),
            pct("flag_duplicado").alias("pct_duplicado"), pct("flag_usable").alias("pct_usable")).toPandas())
        cal_f.to_parquet(C.ORO / "agg_calidad_fuente.parquet", index=False)
        cal_m.to_parquet(C.ORO / "agg_calidad_municipio.parquet", index=False)

        for nombre, d in [("agg_cubo_zona", cubo), ("agg_hist_precio_m2", hist), ("agg_tendencia", tend),
                          ("agg_calidad_fuente", cal_f), ("agg_calidad_municipio", cal_m)]:
            d.to_csv(C.POWERBI / f"{nombre}.csv", index=False, encoding="utf-8-sig")

        # muestra del hecho en CSV para probar Power BI sin cargar el Parquet completo
        muestra = h.where("flag_usable").limit(50000).toPandas()
        muestra.to_csv(C.POWERBI / "hecho_anuncio_muestra_50k.csv", index=False, encoding="utf-8-sig")

        resumen = h.agg(F.count("*").alias("filas_hecho"), F.sum(F.col("flag_usable").cast("int")).alias("filas_usables"),
                        F.sum(F.col("flag_outlier").cast("int")).alias("outliers"),
                        F.sum((F.col("id_ubicacion") == 1).cast("int")).alias("sin_ubicacion"),
                        F.sum((F.col("flag_completo") & ~F.col("flag_duplicado")).cast("int")).alias("completos_unicos")).first().asDict()
        meta = {**resumen, "colonias_con_datos": int(cubo["colonia"].nunique()),
                "municipios_con_datos": int(cubo["municipio"].nunique()),
                "segundos_consulta_cubo_spark": round(seg_cubo, 1), "segundos_etapa": round(time.time() - t0, 1)}
        (C.METRICAS / "etapa3_oro.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
        print(json.dumps(meta, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
