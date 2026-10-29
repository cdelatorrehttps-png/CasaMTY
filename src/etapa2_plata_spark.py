"""
ETAPA 2 - LIMPIEZA Y TRANSFORMACIÓN (Apache Spark local) -> capa PLATA

Lee el crudo de cada portal (esquemas distintos), lo unifica y estandariza:
  1. Unifica nombres de campos (precio, m², recámaras, municipio, colonia, tipo).
  2. Convierte precios en texto / MDP / USD a pesos mexicanos numéricos.
  3. Normaliza texto de municipios y colonias contra el catálogo del AMM.
  4. Separa venta y renta.
  5. Detecta duplicados (recaptura en el mismo portal + mismo inmueble en otro portal).
  6. Calcula precio por m² y marca la calidad del registro (completo / incompleto).
Resultado: lake/plata/ (Parquet) + data/metricas/etapa2_calidad.json
"""
import json
import time

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

import config as C

# --------------------------------------------------------------------------- #
# Esquemas explícitos del crudo (evita una pasada extra de inferencia)
# --------------------------------------------------------------------------- #
D, S, L = T.DoubleType(), T.StringType(), T.LongType()
ESQUEMAS = {
    "inmuebles24": T.StructType([T.StructField(n, t) for n, t in [
        ("id", S), ("titulo", S), ("precio", S), ("tipo_operacion", S), ("tipo_propiedad", S),
        ("m2_construccion", D), ("m2_terreno", D), ("recamaras", D), ("banos", D), ("estacionamientos", D),
        ("municipio", S), ("colonia", S), ("fecha_publicacion", S)]]),
    "lamudi": T.StructType([T.StructField(n, t) for n, t in [
        ("listing_id", S), ("title", S), ("price", D), ("currency", S), ("operation", S), ("property_type", S),
        ("built_area", D), ("land_area", D), ("bedrooms", D), ("bathrooms", D), ("parking", D),
        ("city", S), ("neighborhood", S), ("published_at", S)]]),
    "vivanuncios": T.StructType([T.StructField(n, t) for n, t in [
        ("aid", S), ("titulo", S), ("precio_txt", S), ("operacion", S), ("categoria", S), ("superficie", S),
        ("habitaciones", D), ("banos", D), ("cochera", D), ("zona", S), ("fecha", S)]]),
    "mercadolibre": T.StructType([
        T.StructField("id", S), T.StructField("title", S),
        T.StructField("price", T.StructType([T.StructField("amount", D), T.StructField("currency_id", S)])),
        T.StructField("listing_type", S), T.StructField("category", S),
        T.StructField("atributos", T.StructType([T.StructField(n, D) for n in
                                                  ["COVERED_AREA", "TOTAL_AREA", "BEDROOMS", "FULL_BATHROOMS", "PARKING_LOTS"]])),
        T.StructField("location", T.StructType([T.StructField("city", S), T.StructField("neighborhood", S)])),
        T.StructField("date_created", S)]),
}


# --------------------------------------------------------------------------- #
# Funciones de normalización (mismas reglas para datos y catálogo)
# --------------------------------------------------------------------------- #
def norm(c):
    """Mayúsculas, sin acentos, sin signos, espacios colapsados."""
    c = F.upper(F.trim(c))
    c = F.translate(c, "ÁÉÍÓÚÜÑ", "AEIOUUN")
    c = F.regexp_replace(c, r"[^A-Z0-9 ]", " ")
    c = F.regexp_replace(c, r"\s+", " ")
    return F.trim(c)


def norm_municipio(c):
    return F.regexp_replace(norm(c), r" (N L|NL|NUEVO LEON)$", "")


def norm_colonia(c):
    return F.regexp_replace(norm(c), r"^(COLONIA|COL) ", "")


def parsear_precio(txt):
    """Texto de precio -> (monto, moneda). Soporta '$4.50 MDP', '4,500,000', 'USD 250,000'."""
    monto = F.regexp_replace(txt, r"[^0-9.]", "").cast("double")
    monto = F.when(txt.rlike(r"(?i)MDP|MILLONES"), monto * 1_000_000).otherwise(monto)
    moneda = F.when(txt.rlike(r"(?i)USD|US\$|DLS"), F.lit("USD")).otherwise(F.lit("MXN"))
    return monto, moneda


def leer_portal(spark, portal):
    ruta = str(C.BRONCE / portal / "corrida=*" / "*.jsonl.gz")
    df = (spark.read.schema(ESQUEMAS[portal]).json(ruta)
          .select("*", F.col("_metadata.file_path").alias("archivo")))
    f = F.lit(portal).alias("fuente")
    num = lambda c: c.cast("int")
    if portal == "inmuebles24":
        monto, moneda = parsear_precio(F.col("precio"))
        tipo = F.lower(F.col("tipo_propiedad"))
        return df.select(
            f, F.col("id").alias("id_anuncio"), F.col("fecha_publicacion"), F.col("archivo"),
            F.lower("tipo_operacion").alias("operacion"), tipo.alias("tipo"),
            monto.alias("monto"), moneda.alias("moneda"),
            F.when(tipo == "terreno", F.col("m2_terreno")).otherwise(F.col("m2_construccion")).alias("m2"),
            num(F.col("recamaras")).alias("recamaras"), num(F.col("banos")).alias("banos"),
            num(F.col("estacionamientos")).alias("estacionamientos"),
            F.col("municipio").alias("municipio_raw"), F.col("colonia").alias("colonia_raw"))
    if portal == "lamudi":
        tipo = (F.when(F.col("property_type") == "house", "casa")
                .when(F.col("property_type") == "apartment", "departamento").otherwise("terreno"))
        return df.select(
            f, F.col("listing_id").alias("id_anuncio"), F.col("published_at").alias("fecha_publicacion"), F.col("archivo"),
            F.when(F.col("operation") == "sale", "venta").otherwise("renta").alias("operacion"), tipo.alias("tipo"),
            F.col("price").alias("monto"), F.col("currency").alias("moneda"),
            F.when(tipo == "terreno", F.col("land_area")).otherwise(F.col("built_area")).alias("m2"),
            num(F.col("bedrooms")).alias("recamaras"), num(F.col("bathrooms")).alias("banos"),
            num(F.col("parking")).alias("estacionamientos"),
            F.col("city").alias("municipio_raw"), F.col("neighborhood").alias("colonia_raw"))
    if portal == "vivanuncios":
        monto, moneda = parsear_precio(F.col("precio_txt"))
        partes = F.split(F.col("zona"), ", ")
        n = F.size(partes)
        return df.select(
            f, F.col("aid").alias("id_anuncio"), F.col("fecha").alias("fecha_publicacion"), F.col("archivo"),
            F.when(F.col("operacion") == "venta", "venta").otherwise("renta").alias("operacion"),
            F.when(F.col("categoria") == "casas", "casa").when(F.col("categoria") == "departamentos", "departamento")
            .otherwise("terreno").alias("tipo"),
            monto.alias("monto"), moneda.alias("moneda"),
            F.regexp_extract(F.col("superficie"), r"(\d+(?:\.\d+)?)", 1).cast("double").alias("m2"),
            num(F.col("habitaciones")).alias("recamaras"), num(F.col("banos")).alias("banos"),
            num(F.col("cochera")).alias("estacionamientos"),
            F.when(n >= 2, partes.getItem(n - 1)).otherwise(F.when(n == 1, partes.getItem(0))).alias("municipio_raw"),
            F.when(n >= 2, partes.getItem(0)).alias("colonia_raw"))
    # mercadolibre
    tipo = (F.when(F.col("category") == "house", "casa")
            .when(F.col("category") == "apartment", "departamento").otherwise("terreno"))
    return df.select(
        f, F.col("id").alias("id_anuncio"), F.col("date_created").alias("fecha_publicacion"), F.col("archivo"),
        F.when(F.col("listing_type") == "sale", "venta").otherwise("renta").alias("operacion"), tipo.alias("tipo"),
        F.col("price.amount").alias("monto"), F.col("price.currency_id").alias("moneda"),
        F.when(tipo == "terreno", F.col("atributos.TOTAL_AREA")).otherwise(F.col("atributos.COVERED_AREA")).alias("m2"),
        num(F.col("atributos.BEDROOMS")).alias("recamaras"), num(F.col("atributos.FULL_BATHROOMS")).alias("banos"),
        num(F.col("atributos.PARKING_LOTS")).alias("estacionamientos"),
        F.col("location.city").alias("municipio_raw"), F.col("location.neighborhood").alias("colonia_raw"))


def catalogos(spark):
    alias, colonias = [], []
    for muni, info in C.MUNICIPIOS.items():
        for a in [muni] + info["aliases"]:
            alias.append((a, muni))
        for nom, _, zona, _ in info["colonias"]:
            colonias.append((muni, nom, zona))
    df_alias = (spark.createDataFrame(alias, ["alias", "municipio"])
                .select(norm_municipio(F.col("alias")).alias("municipio_key"), "municipio").distinct())
    df_col = (spark.createDataFrame(colonias, ["municipio", "colonia", "zona"])
              .withColumn("colonia_key", norm_colonia(F.col("colonia"))))
    return df_alias, df_col


def main():
    t0 = time.time()
    C.asegurar_carpetas()
    spark = C.crear_spark("CasaMTY-plata")
    try:
        crudo = None
        for portal in C.PORTALES:
            d = leer_portal(spark, portal)
            crudo = d if crudo is None else crudo.unionByName(d)

        df = (crudo
              .withColumn("fecha_captura", F.to_date(F.regexp_extract("archivo", r"corrida=([0-9-]{10})", 1)))
              .withColumn("precio_mxn", F.when(F.col("moneda") == "USD", F.col("monto") * C.FX_USD_MXN).otherwise(F.col("monto")))
              .withColumn("municipio_key", norm_municipio(F.col("municipio_raw")))
              .withColumn("colonia_key", norm_colonia(F.col("colonia_raw"))))

        df_alias, df_col = catalogos(spark)
        df = df.join(F.broadcast(df_alias), "municipio_key", "left")
        df = df.join(F.broadcast(df_col), ["municipio", "colonia_key"], "left")   # colonia queda nula si no hay match

        es_terreno = F.col("tipo") == "terreno"
        m2_ok = F.when(es_terreno, F.col("m2").between(*C.M2_TERRENO)).otherwise(F.col("m2").between(*C.M2_VIVIENDA))
        df = (df
              .withColumn("flag_municipio_valido", F.col("municipio").isNotNull())
              .withColumn("flag_colonia_valida", F.col("colonia").isNotNull())
              .withColumn("flag_m2_valido", F.coalesce(m2_ok, F.lit(False)))
              .withColumn("flag_precio_valido", F.coalesce(F.col("precio_mxn") > 0, F.lit(False)))
              .withColumn("flag_completo", F.col("flag_municipio_valido") & F.col("flag_colonia_valida")
                          & F.col("flag_m2_valido") & F.col("flag_precio_valido")))

        # --- duplicados -------------------------------------------------------
        # (a) recaptura: mismo portal + mismo id en varias corridas -> se queda la más reciente
        w1 = Window.partitionBy("fuente", "id_anuncio").orderBy(F.col("fecha_captura").desc())
        df = df.withColumn("flag_dup_recaptura", F.row_number().over(w1) > 1)
        # (b) mismo inmueble en otro portal: zona + tipo + operación + m² + recámaras + precio
        clave = F.concat_ws("|", "municipio", "colonia", "tipo", "operacion", F.round("m2"),
                            F.col("recamaras").cast("string"), F.round("precio_mxn"))
        candidato = F.col("flag_completo") & ~F.col("flag_dup_recaptura")
        df = df.withColumn("clave_dup", F.when(candidato, clave)
                           .otherwise(F.concat_ws("|", F.lit("unico"), "fuente", "id_anuncio", F.col("fecha_captura").cast("string"))))
        w2 = Window.partitionBy("clave_dup").orderBy("fecha_captura", "fuente", "id_anuncio")
        df = df.withColumn("flag_dup_interportal", candidato & (F.row_number().over(w2) > 1))
        df = df.withColumn("flag_duplicado", F.col("flag_dup_recaptura") | F.col("flag_dup_interportal"))

        df = df.withColumn("id_anuncio_global", F.xxhash64("fuente", "id_anuncio", "fecha_captura"))
        df = df.withColumn("precio_m2", F.when(F.col("flag_m2_valido") & F.col("flag_precio_valido"),
                                               F.col("precio_mxn") / F.col("m2")))
        plata = df.select(
            "id_anuncio_global", "fuente", "id_anuncio", "fecha_captura", F.to_date("fecha_publicacion").alias("fecha_publicacion"),
            "operacion", "tipo", "municipio", "colonia", "zona", "moneda", "precio_mxn", "m2", "recamaras", "banos",
            "estacionamientos", "precio_m2", "flag_municipio_valido", "flag_colonia_valida", "flag_m2_valido",
            "flag_precio_valido", "flag_completo", "flag_dup_recaptura", "flag_dup_interportal", "flag_duplicado")

        t1 = time.time()
        plata.repartition(8).write.mode("overwrite").parquet(str(C.PLATA))
        seg_escritura = time.time() - t1

        # --- métricas de calidad (Veracidad) ----------------------------------
        p = spark.read.parquet(str(C.PLATA))
        agg = lambda c: F.avg(F.col(c).cast("int")) * 100
        por_fuente = (p.groupBy("fuente").agg(
            F.count("*").alias("n"), agg("flag_completo").alias("pct_completo"), agg("flag_m2_valido").alias("pct_m2_valido"),
            agg("flag_colonia_valida").alias("pct_colonia_valida"), agg("flag_precio_valido").alias("pct_precio_valido"),
            agg("flag_duplicado").alias("pct_duplicado")).orderBy("fuente").collect())
        tot = p.agg(
            F.count("*").alias("n"), F.sum(F.col("flag_dup_recaptura").cast("int")).alias("dup_recaptura"),
            F.sum(F.col("flag_dup_interportal").cast("int")).alias("dup_interportal"),
            F.sum(F.col("flag_duplicado").cast("int")).alias("duplicados"),
            F.sum((~F.col("flag_duplicado")).cast("int")).alias("unicos"),
            agg("flag_completo").alias("pct_completo"), agg("flag_m2_valido").alias("pct_m2_valido"),
            agg("flag_colonia_valida").alias("pct_colonia_valida"), agg("flag_precio_valido").alias("pct_precio_valido"),
            agg("flag_municipio_valido").alias("pct_municipio_valido"),
            F.sum((F.col("tipo") == "terreno").cast("int")).alias("terrenos"),
            F.sum((F.col("moneda") == "USD").cast("int")).alias("anuncios_en_usd")).first().asDict()
        r = lambda x: round(float(x), 2)
        calidad = {
            "registros_crudos": tot["n"], "registros_unicos": tot["unicos"],
            "duplicados_recaptura": tot["dup_recaptura"], "duplicados_interportal": tot["dup_interportal"],
            "tasa_duplicados_pct": r(tot["duplicados"] / tot["n"] * 100),
            "pct_completo": r(tot["pct_completo"]), "pct_m2_valido": r(tot["pct_m2_valido"]),
            "pct_colonia_valida": r(tot["pct_colonia_valida"]), "pct_precio_valido": r(tot["pct_precio_valido"]),
            "pct_municipio_valido": r(tot["pct_municipio_valido"]),
            "terrenos": tot["terrenos"], "anuncios_en_usd": tot["anuncios_en_usd"],
            "por_fuente": [{k: (r(v) if isinstance(v, float) else v) for k, v in row.asDict().items()} for row in por_fuente],
            "segundos_etapa": round(time.time() - t0, 1), "segundos_escritura_parquet": round(seg_escritura, 1),
        }
        (C.METRICAS / "etapa2_calidad.json").write_text(json.dumps(calidad, indent=2, ensure_ascii=False))
        print(json.dumps({k: v for k, v in calidad.items() if k != "por_fuente"}, indent=2, ensure_ascii=False))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
