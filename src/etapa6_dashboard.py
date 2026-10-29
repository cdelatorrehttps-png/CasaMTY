"""
ETAPA 6 - DASHBOARD OPERATIVO + GRÁFICAS PARA EL REPORTE

Construye dashboard/index.html (un solo archivo, funciona sin internet y sin servidor: basta
con abrirlo en el navegador) a partir de las tablas de la capa oro, y guarda gráficas PNG en
docs/img/ para el reporte técnico.

El tablero de Power BI se arma sobre las mismas tablas (carpeta powerbi/ y docs/guia_powerbi.md);
este HTML es la versión "lista para mostrar" que no requiere instalar nada.
"""
import json
import time
from datetime import date

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

import config as C

COLOR = {"venta": "#0f8a5f", "renta": "#2b6cb0", "gris": "#9bb8ab", "rojo": "#c0392b"}


def leer_json(nombre, defecto=None):
    ruta = C.METRICAS / nombre
    return json.loads(ruta.read_text()) if ruta.exists() else (defecto or {})


def main():
    t0 = time.time()
    C.asegurar_carpetas()
    cubo = pd.read_parquet(C.ORO / "agg_cubo_zona.parquet")
    hist = pd.read_parquet(C.ORO / "agg_hist_precio_m2.parquet")
    tend = pd.read_parquet(C.ORO / "agg_tendencia.parquet")
    est = pd.read_parquet(C.ORO / "hecho_estimacion.parquet")
    dim_ub = pd.read_parquet(C.ORO / "dim_ubicacion.parquet")
    pred = pd.read_parquet(C.ORO / "predicciones_muestra.parquet")
    m_cal = leer_json("etapa2_calidad.json"); m_oro = leer_json("etapa3_oro.json")
    m_mod = leer_json("etapa4_modelo.json"); m_wh = leer_json("etapa5_warehouse.json")
    m_ing = leer_json("etapa1_ingesta.json"); tiempos = leer_json("tiempos_etapas.json")

    municipios = list(C.MUNICIPIOS)
    colonias = [(r.colonia, municipios.index(r.municipio)) for r in dim_ub.itertuples()
                if r.colonia != "SIN COLONIA" and r.municipio in municipios]
    col_idx = {(r.municipio, r.colonia): i for i, r in enumerate(
        [x for x in dim_ub.itertuples() if x.colonia != "SIN COLONIA" and x.municipio in municipios])}
    mi = {m: i for i, m in enumerate(municipios)}
    oi = {"venta": 0, "renta": 1}; ti = {"casa": 0, "departamento": 1}

    cubo = cubo[cubo["colonia"].isin([c[0] for c in colonias])]
    cubo_l = [[mi[r.municipio], col_idx[(r.municipio, r.colonia)], oi[r.operacion], ti[r.tipo], int(r.m2b), int(r.recb),
               int(r.n), int(r.ppm2_mediana), int(r.ppm2_p25), int(r.ppm2_p75), int(r.precio_mediano)]
              for r in cubo.itertuples()]
    hist_l = [[mi[r.municipio], oi[r.operacion], ti[r.tipo], int(r.bin), int(r.n), int(r.step)] for r in hist.itertuples()
              if r.municipio in mi]
    tend_l = [[r.fecha_captura, mi[r.municipio], oi[r.operacion], ti[r.tipo], int(r.n), int(r.ppm2_mediana)]
              for r in tend.itertuples() if r.municipio in mi]
    # estimación por colonia
    ub_by_id = {r.id_ubicacion: (r.municipio, r.colonia) for r in dim_ub.itertuples()}
    est_l = []
    for r in est.itertuples():
        mu, co = ub_by_id[r.id_ubicacion]
        if (mu, co) in col_idx and r.id_tipo in (1, 2):
            est_l.append([col_idx[(mu, co)], int(r.id_tipo) - 1, int(r.id_operacion) - 1, int(r.n_anuncios_usados),
                          float(r.precio_m2_estimado_modelo), None if pd.isna(r.mape_zona) else float(r.mape_zona)])
    sc = pred[pred["tipo"].isin(ti)]
    scatter_l = [[oi[r.operacion], float(r.precio_mxn), float(r.est_modelo)] for r in sc.itertuples()]

    # ---------------- metas / indicadores ----------------
    seg_total = sum(v for k, v in tiempos.items() if k in ("etapa1", "etapa2", "etapa3", "etapa4", "etapa5"))
    min_total = seg_total / 60 if seg_total else None
    res = m_mod.get("resultados", {})
    mape_v = res.get("venta", {}).get("modelo", {}).get("mape_pct"); mape_r = res.get("renta", {}).get("modelo", {}).get("mape_pct")
    ok_mape = mape_v is not None and mape_v < C.META_MAPE and mape_r < C.META_MAPE
    metas = [
        ["Registros crudos procesados", "≥ 1,000,000", f"{m_cal.get('registros_crudos', 0):,}", m_cal.get("registros_crudos", 0) >= 1_000_000],
        ["Completitud de campos clave", f"> {C.META_COMPLETITUD:.0f}%", f"{m_cal.get('pct_completo')}%", (m_cal.get("pct_completo") or 0) > C.META_COMPLETITUD],
        ["Tasa de duplicados detectados", "registrar % y bajarlo", f"{m_cal.get('tasa_duplicados_pct')}% ({m_cal.get('duplicados_recaptura', 0):,} recapturas + {m_cal.get('duplicados_interportal', 0):,} entre portales)", True],
        ["MAPE del estimado (prueba)", f"< {C.META_MAPE:.0f}%", f"venta {mape_v}% · renta {mape_r}%", ok_mape],
        ["Zonas con ≥ 30 anuncios que cumplen MAPE < 20%", "mayoría", f"{m_mod.get('pct_zonas_suficientes_que_cumplen_meta')}% de {m_mod.get('zonas_con_suficiente_muestra')} zonas", (m_mod.get("pct_zonas_suficientes_que_cumplen_meta") or 0) >= 80],
        ["Tiempo del pipeline completo", f"< {C.META_PIPELINE_MIN:.0f} min", f"{min_total:.1f} min (ingesta simulada + Spark + modelo + warehouse)" if min_total else "—", bool(min_total) and min_total < C.META_PIPELINE_MIN],
        ["Cobertura AMM", "≥ 8 municipios con datos", f"{m_oro.get('municipios_con_datos')} municipios · {m_oro.get('colonias_con_datos')} colonias", (m_oro.get("municipios_con_datos") or 0) >= 8],
        ["Costo de licencias", "$0", "$0 (todo gratuito y local)", True],
        ["Costo por consulta analítica", "informativo", f"{m_wh.get('segundos_promedio_por_consulta', '—')} s promedio · $0", True],
        ["Valor de uso del tablero", "≤ 3 clics: municipio → colonia → precio/m²", "2 selecciones (municipio, colonia)", True],
    ]
    embudo = [["Registros crudos", m_cal.get("registros_crudos", 0)], ["Sin duplicados", m_cal.get("registros_unicos", 0)],
              ["Completos y únicos", m_oro.get("completos_unicos", 0)],
              ["Viviendas sin atípicos (usables)", m_oro.get("filas_usables", 0)]]

    data = {
        "municipios": municipios, "colonias": colonias, "cubo": cubo_l, "hist": hist_l, "tend": tend_l,
        "est": est_l, "scatter": scatter_l, "umbral": C.UMBRAL_MUESTRA_ZONA, "m2_buckets": [list(b) for b in C.M2_BUCKETS],
        "modelo": {k: {kk: vv for kk, vv in v.items()} for k, v in res.items()},
        "calidad": m_cal, "metas": {"mape": C.META_MAPE}, "metas_tabla": metas, "embudo": embudo,
        "municipios_con_datos": m_oro.get("municipios_con_datos"), "generado": date.today().strftime("%d/%m/%Y"),
    }
    plantilla = (C.RAIZ / "src" / "plantilla_dashboard.html").read_text(encoding="utf-8")
    html = plantilla.replace("/*__DATA__*/", "const DATA = " + json.dumps(data, ensure_ascii=False, separators=(",", ":")) + ";")
    (C.DASHBOARD / "index.html").write_text(html, encoding="utf-8")
    print(f"[dashboard] dashboard/index.html ({len(html)/1e6:.2f} MB)")

    # ---------------- gráficas PNG para el reporte ----------------
    plt.rcParams.update({"font.size": 10, "axes.spines.top": False, "axes.spines.right": False})
    # 1. precio mediano por municipio (casas, venta)
    c = cubo[(cubo["operacion"] == "venta") & (cubo["tipo"] == "casa")]
    g = c.groupby("municipio").apply(lambda d: (d["ppm2_mediana"] * d["n"]).sum() / d["n"].sum(), include_groups=False).sort_values()
    fig, ax = plt.subplots(figsize=(7, 4.2)); ax.barh(g.index, g.values, color=COLOR["venta"])
    ax.set_title("Precio por m² de casas en venta, por municipio (simulado)"); ax.set_xlabel("MXN por m² (promedio ponderado de medianas de colonia)")
    for i, v in enumerate(g.values): ax.text(v, i, f" ${v:,.0f}", va="center", fontsize=8)
    fig.tight_layout(); fig.savefig(C.IMG / "01_precio_m2_municipio.png", dpi=150); plt.close(fig)
    # 2. embudo
    fig, ax = plt.subplots(figsize=(7, 3.2)); nombres = [e[0] for e in embudo][::-1]; vals = [e[1] for e in embudo][::-1]
    ax.barh(nombres, vals, color=COLOR["gris"]); ax.set_title("Embudo de limpieza de datos")
    for i, v in enumerate(vals): ax.text(v, i, f" {v:,}", va="center", fontsize=9)
    fig.tight_layout(); fig.savefig(C.IMG / "02_embudo_limpieza.png", dpi=150); plt.close(fig)
    # 3. MAPE comparativo
    fig, axs = plt.subplots(1, 2, figsize=(8, 3.4), sharey=True)
    for ax, op in zip(axs, ["venta", "renta"]):
        r = res.get(op, {}); v = [r["a_ojo_promedio_municipio"]["mape_pct"], r["mediana_m2_municipio"]["mape_pct"], r["modelo"]["mape_pct"]]
        b = ax.bar(["A ojo", "Mediana\npor m²", "Modelo"], v, color=[COLOR["gris"], COLOR["gris"], COLOR[op]])
        ax.set_title(op.capitalize()); ax.axhline(C.META_MAPE, color=COLOR["rojo"], ls="--", lw=1)
        for rect, x in zip(b, v): ax.text(rect.get_x() + rect.get_width() / 2, x, f"{x:.1f}%", ha="center", va="bottom", fontsize=9)
    axs[0].set_ylabel("MAPE (%)"); fig.suptitle("Error del estimado vs. métodos tradicionales (línea roja = meta 20%)", fontsize=10)
    fig.tight_layout(); fig.savefig(C.IMG / "03_mape_comparativo.png", dpi=150); plt.close(fig)
    # 4. real vs estimado
    fig, axs = plt.subplots(1, 2, figsize=(8, 3.8))
    for ax, op in zip(axs, ["venta", "renta"]):
        s = pred[pred["operacion"] == op]
        ax.scatter(s["precio_mxn"], s["est_modelo"], s=3, alpha=.35, color=COLOR[op])
        lim = [s["precio_mxn"].min(), s["precio_mxn"].max()]; ax.plot(lim, lim, "k--", lw=.8)
        ax.set_xscale("log"); ax.set_yscale("log"); ax.set_title(op.capitalize()); ax.set_xlabel("Publicado (MXN)")
    axs[0].set_ylabel("Estimado (MXN)"); fig.tight_layout(); fig.savefig(C.IMG / "04_real_vs_estimado.png", dpi=150); plt.close(fig)
    # 5. calidad por portal
    pf = pd.DataFrame(m_cal.get("por_fuente", []))
    if len(pf):
        fig, ax = plt.subplots(figsize=(7, 3.4)); x = range(len(pf)); w = .27
        ax.bar([i - w for i in x], pf["pct_completo"], w, label="% completo", color=COLOR["venta"])
        ax.bar(list(x), pf["pct_colonia_valida"], w, label="% colonia válida", color=COLOR["renta"])
        ax.bar([i + w for i in x], pf["pct_duplicado"], w, label="% duplicado", color=COLOR["rojo"])
        ax.set_xticks(list(x)); ax.set_xticklabels(pf["fuente"]); ax.legend(fontsize=8); ax.set_title("Calidad de datos por portal")
        fig.tight_layout(); fig.savefig(C.IMG / "05_calidad_portal.png", dpi=150); plt.close(fig)
    (C.METRICAS / "etapa6_dashboard.json").write_text(json.dumps({"segundos_etapa": round(time.time() - t0, 1), "pipeline_total_min": min_total}, indent=2))


if __name__ == "__main__":
    main()
