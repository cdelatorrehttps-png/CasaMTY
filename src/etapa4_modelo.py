"""
ETAPA 4 - MODELO SIMPLE DE ESTIMACIÓN DE PRECIO DE LISTADO

Pregunta analítica (Fase 1): ¿se puede estimar el precio de listado de una vivienda del AMM
con un error aceptable para orientación usando solo atributos publicados?

Se entrena un árbol potenciado por gradiente (HistGradientBoostingRegressor de scikit-learn)
por tipo de operación (venta y renta NUNCA se mezclan) y se compara contra dos métodos
"tradicionales":
  A) "a ojo"        : promedio del precio de listado por municipio x tipo (lo que haría alguien en Excel)
  B) mediana por m² : mediana de precio/m² del municipio x tipo, multiplicada por los m² del anuncio

Entrada : lake/oro/hecho_anuncio (solo flag_usable = completo, sin duplicado, sin outlier, sin terrenos)
Salida  : lake/oro/hecho_estimacion.parquet, lake/oro/predicciones_muestra.parquet,
          data/metricas/etapa4_modelo.json
"""
import json
import time

import numpy as np
import pandas as pd
import pyarrow.dataset as ds
from sklearn.ensemble import HistGradientBoostingRegressor
from sklearn.inspection import permutation_importance

import config as C

FEATURES = ["id_municipio", "id_colonia", "id_tipo", "m2", "recamaras", "banos", "estacionamientos"]
MAX_TRAIN = 600_000


def metricas(real, est):
    ape = np.abs(est - real) / real * 100
    return {"mae_mxn": round(float(np.mean(np.abs(est - real))), 0), "mape_pct": round(float(np.mean(ape)), 2),
            "mdape_pct": round(float(np.median(ape)), 2),
            "pct_dentro_20": round(float(np.mean(ape <= 20) * 100), 1)}


def main():
    t0 = time.time()
    C.asegurar_carpetas()
    rng = np.random.default_rng(C.SEMILLA)
    dim_ub = pd.read_parquet(C.ORO / "dim_ubicacion.parquet")
    dim_tipo = pd.read_parquet(C.ORO / "dim_inmueble_tipo.parquet")
    dim_op = pd.read_parquet(C.ORO / "dim_operacion.parquet")

    cols = ["id_ubicacion", "id_tipo", "id_operacion", "m2", "recamaras", "banos", "estacionamientos", "precio_mxn", "precio_m2"]
    df = ds.dataset(str(C.ORO / "hecho_anuncio"), format="parquet").to_table(
        columns=cols, filter=ds.field("flag_usable") == True).to_pandas()   # noqa: E712
    df = df.merge(dim_ub, on="id_ubicacion")
    # ids categóricos 0..K-1 (HistGradientBoosting soporta hasta 255 categorías)
    df["id_municipio"] = df["municipio"].astype("category").cat.codes
    df["id_colonia"] = df["id_ubicacion"].astype("category").cat.codes
    df["id_tipo"] = df["id_tipo"].astype(int) - 1
    df = df.reset_index(drop=True)
    print(f"[modelo] filas usables: {len(df):,}")

    resultados, preds_test, importancias = {}, [], {}
    seg_entrenamiento = 0.0
    for id_op, nombre_op in zip(dim_op["id_operacion"], dim_op["operacion"]):
        d = df[df["id_operacion"] == id_op].copy()
        idx = rng.permutation(len(d))
        n_test = int(len(d) * 0.2)
        test = d.iloc[idx[:n_test]]
        train = d.iloc[idx[n_test:]]
        if len(train) > MAX_TRAIN:
            train = train.sample(MAX_TRAIN, random_state=C.SEMILLA)

        # --- métodos tradicionales ---
        g_mean = train.groupby(["municipio", "id_tipo"])["precio_mxn"].mean().rename("a_ojo")
        g_med = train.groupby(["municipio", "id_tipo"])["precio_m2"].median().rename("med_ppm2")
        t = test.join(g_mean, on=["municipio", "id_tipo"]).join(g_med, on=["municipio", "id_tipo"])
        t["est_a_ojo"] = t["a_ojo"]
        t["est_mediana_m2"] = t["med_ppm2"] * t["m2"]

        # --- modelo ---
        cat_mask = [f in ("id_municipio", "id_colonia", "id_tipo") for f in FEATURES]
        modelo = HistGradientBoostingRegressor(max_iter=250, learning_rate=0.1, max_leaf_nodes=48,
                                               categorical_features=cat_mask, random_state=C.SEMILLA)
        t1 = time.time()
        modelo.fit(train[FEATURES], np.log(train["precio_mxn"]))
        seg = time.time() - t1
        seg_entrenamiento += seg
        t["est_modelo"] = np.exp(modelo.predict(t[FEATURES]))

        resultados[nombre_op] = {
            "filas_entrenamiento": int(len(train)), "filas_prueba": int(len(t)), "segundos_entrenamiento": round(seg, 1),
            "a_ojo_promedio_municipio": metricas(t["precio_mxn"].values, t["est_a_ojo"].values),
            "mediana_m2_municipio": metricas(t["precio_mxn"].values, t["est_mediana_m2"].values),
            "modelo": metricas(t["precio_mxn"].values, t["est_modelo"].values)}
        m_base = resultados[nombre_op]["a_ojo_promedio_municipio"]["mape_pct"]
        m_mod = resultados[nombre_op]["modelo"]["mape_pct"]
        resultados[nombre_op]["reduccion_error_vs_a_ojo_pct"] = round((m_base - m_mod) / m_base * 100, 1)
        m_b2 = resultados[nombre_op]["mediana_m2_municipio"]["mape_pct"]
        resultados[nombre_op]["reduccion_error_vs_mediana_m2_pct"] = round((m_b2 - m_mod) / m_b2 * 100, 1)

        # importancia por permutación (muestra pequeña para que sea rápido)
        s = t.sample(min(15000, len(t)), random_state=C.SEMILLA)
        pi = permutation_importance(modelo, s[FEATURES], np.log(s["precio_mxn"]), n_repeats=3, random_state=C.SEMILLA, n_jobs=1)
        importancias[nombre_op] = {f: round(float(v), 4) for f, v in zip(FEATURES, pi.importances_mean)}

        # predicción para TODAS las filas usables de esta operación (estimado por zona)
        d["est_modelo"] = np.exp(modelo.predict(d[FEATURES]))
        d["est_ppm2"] = d["est_modelo"] / d["m2"]
        d["en_test"] = False
        d.loc[test.index, "en_test"] = True
        d["ape"] = np.nan
        d.loc[test.index, "ape"] = (np.abs(t["est_modelo"] - t["precio_mxn"]) / t["precio_mxn"] * 100).values
        preds_test.append(d)
        print(f"[modelo] {nombre_op}: MAPE modelo {m_mod}% | a ojo {m_base}% | mediana/m² {m_b2}%  ({seg:.0f} s)")

    todo = pd.concat(preds_test, ignore_index=True)

    # --- hecho_estimacion por zona (colonia) x tipo x operación ---
    g = todo.groupby(["id_ubicacion", "id_tipo", "id_operacion"])
    est = g.agg(n_anuncios_usados=("precio_m2", "size"),
                precio_m2_mediana_observada=("precio_m2", "median"),
                precio_m2_estimado_modelo=("est_ppm2", "median"),
                n_prueba=("ape", "count"), mape_zona=("ape", "mean")).reset_index()
    est["id_tipo"] = est["id_tipo"] + 1
    est["suficiente_muestra"] = est["n_anuncios_usados"] >= C.UMBRAL_MUESTRA_ZONA
    est["cumple_meta_mape"] = est["suficiente_muestra"] & (est["mape_zona"] < C.META_MAPE)
    est["fecha_modelo"] = str(pd.Timestamp.now().date())
    for c in ["precio_m2_mediana_observada", "precio_m2_estimado_modelo", "mape_zona"]:
        est[c] = est[c].round(2)
    est.to_parquet(C.ORO / "hecho_estimacion.parquet", index=False)
    est.to_csv(C.POWERBI / "hecho_estimacion.csv", index=False, encoding="utf-8-sig")

    # muestra para el gráfico real vs estimado
    t_only = todo[todo["en_test"]].merge(dim_op, on="id_operacion").merge(dim_tipo[["id_tipo", "tipo"]].assign(id_tipo=lambda x: x["id_tipo"] - 1), on="id_tipo")
    mu = t_only.sample(min(4000, len(t_only)), random_state=C.SEMILLA)[["municipio", "operacion", "tipo", "m2", "precio_mxn", "est_modelo"]]
    mu.round(0).to_parquet(C.ORO / "predicciones_muestra.parquet", index=False)

    suf = est[est["suficiente_muestra"] & (est["n_prueba"] >= 10)]
    out = {"resultados": resultados, "importancia_permutacion": importancias,
           "zonas_con_suficiente_muestra": int(est["suficiente_muestra"].sum()), "zonas_totales": int(len(est)),
           "zonas_que_cumplen_meta_mape": int(est["cumple_meta_mape"].sum()),
           "pct_zonas_suficientes_que_cumplen_meta": round(float((suf["mape_zona"] < C.META_MAPE).mean() * 100), 1) if len(suf) else None,
           "segundos_entrenamiento_total": round(seg_entrenamiento, 1), "segundos_etapa": round(time.time() - t0, 1),
           "nota": "Datos SIMULADOS: el error refleja el ruido de la simulación, no el mercado real."}
    (C.METRICAS / "etapa4_modelo.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
    print(json.dumps({k: v for k, v in out.items() if k not in ("resultados",)}, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
