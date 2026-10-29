"""
ETAPA 1 - INGESTA (batch) -> Data Lake / capa BRONCE

Genera el dataset de práctica SIMULADO de anuncios de vivienda del AMM (>= 1 millón
de registros crudos) y lo deja en lake/bronce/<portal>/corrida=<fecha>/part-XXXX.jsonl.gz
"tal como salió del portal": cada portal usa nombres de campo, formatos de precio y
estructura distintos (Variedad), e incluye suciedad controlada (Veracidad):
  - m² faltantes o inflados, precio faltante, recámaras/baños faltantes
  - colonias mal escritas / sin acentos / con prefijo "Col." / faltantes
  - municipio escrito de varias formas
  - precios en texto ("$4.50 MDP", "4,500,000", "USD 250,000")
  - duplicados: recaptura en la semana siguiente y el mismo inmueble en otro portal
  - precio de renta capturado en anuncios de venta

NO se hace scraping real: la Fase 2 definió que, si el portal no se puede recorrer
de forma ética/estable, se usa un dataset de práctica con el mismo esquema.
Todo está etiquetado como SIMULADO.
"""
import gzip
import json
import shutil
import time
import unicodedata
from datetime import date, timedelta

import numpy as np

import config as C

TIPOS = ["casa", "departamento", "terreno"]


def quitar_acentos(s):
    return "".join(ch for ch in unicodedata.normalize("NFD", s) if unicodedata.category(ch) != "Mn")


def lista_int(a):
    """Array float con NaN -> lista de int/None (para JSON)."""
    return [None if x != x else int(x) for x in a.tolist()]


# --------------------------------------------------------------------------- #
# 1. Universo de inmuebles únicos (verdad "de laboratorio")
# --------------------------------------------------------------------------- #
def generar_universo(n, rng):
    muni_names = list(C.MUNICIPIOS)
    pesos = np.array([C.MUNICIPIOS[m]["peso"] for m in muni_names])
    pesos = pesos / pesos.sum()
    muni = rng.choice(len(muni_names), n, p=pesos)

    # catálogo global de colonias
    col_muni, col_nom, col_fac, col_zona = [], [], [], []
    por_muni = {}
    for i, m in enumerate(muni_names):
        ids = []
        for nom, fac, zona, peso in C.MUNICIPIOS[m]["colonias"]:
            ids.append(len(col_nom))
            col_muni.append(i); col_nom.append(nom); col_fac.append(fac); col_zona.append(zona)
        w = np.array([c[3] for c in C.MUNICIPIOS[m]["colonias"]], dtype=float)
        por_muni[i] = (np.array(ids), w / w.sum())
    col_fac = np.array(col_fac)

    col = np.zeros(n, dtype=np.int64)
    for i in range(len(muni_names)):
        idx = np.where(muni == i)[0]
        ids, w = por_muni[i]
        col[idx] = rng.choice(ids, len(idx), p=w)

    operacion = (rng.random(n) >= 0.62).astype(np.int8)          # 0 venta, 1 renta
    u = rng.random(n)
    tipo = np.where(operacion == 0,
                    np.where(u < 0.55, 0, np.where(u < 0.93, 1, 2)),
                    np.where(u < 0.45, 0, 1)).astype(np.int8)      # sin terrenos en renta

    # tamaño (m² de construcción; para terreno es m² de terreno)
    m2 = np.empty(n)
    for t, (mu, sg, lo, hi) in enumerate([(160, 0.35, 45, 900), (85, 0.35, 35, 350), (250, 0.60, 90, 5000)]):
        idx = tipo == t
        m2[idx] = np.clip(rng.lognormal(np.log(mu), sg, idx.sum()), lo, hi)
    m2 = np.round(m2)

    rec = np.full(n, np.nan)
    rc = tipo == 0
    rec[rc] = np.clip(np.round(1 + (m2[rc] - 35) / 48 + rng.normal(0, 0.6, rc.sum())), 1, 6)
    rd = tipo == 1
    rec[rd] = np.clip(np.round(1 + (m2[rd] - 30) / 38 + rng.normal(0, 0.5, rd.sum())), 1, 4)
    viv = tipo < 2
    banos = np.full(n, np.nan)
    banos[viv] = np.clip(np.round(rec[viv] * 0.7 + 0.5 + rng.normal(0, 0.5, viv.sum())), 1, 6)
    estac = np.full(n, np.nan)
    estac[rc] = np.clip(np.round(rec[rc] * 0.6 + rng.normal(0, 0.7, rc.sum())), 0, 5)
    estac[rd] = np.clip(np.round(rng.normal(1.2, 0.7, rd.sum())), 0, 3)
    m2_terreno = np.where(tipo == 0, np.round(m2 * rng.uniform(1.0, 1.8, n)), np.where(tipo == 2, m2, np.nan))

    # precio por m² "verdadero" + ruido de mercado
    base = np.array([C.MUNICIPIOS[m]["base_ppm2"] for m in muni_names])[muni]
    ref = np.array([160.0, 85.0, 250.0])[tipo]
    tf = np.array([1.0, 1.12, 0.55])[tipo]
    recf = np.where(viv, 1 + 0.012 * (np.nan_to_num(rec, nan=3) - 3), 1.0)
    ppm2_limpio = base * col_fac[col] * tf * (m2 / ref) ** -0.10 * recf
    ruido = np.where(operacion == 0, np.exp(rng.normal(0, 0.13, n)),
                     0.0048 * np.exp(rng.normal(0, 0.16, n)))
    precio = ppm2_limpio * ruido * m2
    precio = np.where(operacion == 0, np.round(precio / 5000) * 5000, np.maximum(np.round(precio / 100) * 100, 3000))

    return dict(n=n, muni=muni, col=col, operacion=operacion, tipo=tipo, m2=m2, rec=rec,
                banos=banos, estac=estac, m2_terreno=m2_terreno, precio=precio,
                muni_names=muni_names, col_muni=np.array(col_muni), col_nom=np.array(col_nom, dtype=object),
                col_zona=np.array(col_zona, dtype=object))


# --------------------------------------------------------------------------- #
# 2. Capturas (filas crudas): base + recapturas + duplicados entre portales
# --------------------------------------------------------------------------- #
def generar_capturas(U, rng):
    n = U["n"]
    n_portales = len(C.PORTALES)
    p_portal = np.array(list(C.PORTALES.values()))
    corrida0 = rng.integers(0, len(C.CORRIDAS), n)
    portal0 = rng.choice(n_portales, n, p=p_portal)

    prop = [np.arange(n)]; corr = [corrida0]; port = [portal0]
    lid = [np.arange(n) * 2]; mult = [np.ones(n)]

    # recaptura (mismo portal, mismo id, semana siguiente; 3% cambia de precio)
    sel = np.where((corrida0 < len(C.CORRIDAS) - 1) & (rng.random(n) < C.P_RECAPTURA))[0]
    prop.append(sel); corr.append(corrida0[sel] + 1); port.append(portal0[sel]); lid.append(sel * 2)
    mult.append(np.where(rng.random(len(sel)) < 0.03, rng.choice([0.95, 0.97, 1.03], len(sel)), 1.0))

    # mismo inmueble en otro portal (id distinto; 30% con precio algo distinto)
    sel2 = np.where(rng.random(n) < C.P_DUP_INTERPORTAL)[0]
    prop.append(sel2); corr.append(corrida0[sel2])
    port.append((portal0[sel2] + rng.integers(1, n_portales, len(sel2))) % n_portales)
    lid.append(sel2 * 2 + 1)
    mult.append(np.where(rng.random(len(sel2)) < 0.30, 1 + rng.choice([-1, 1], len(sel2)) * rng.uniform(0.02, 0.05, len(sel2)), 1.0))

    return dict(prop=np.concatenate(prop), corr=np.concatenate(corr), port=np.concatenate(port),
                lid=np.concatenate(lid), mult=np.concatenate(mult))


# --------------------------------------------------------------------------- #
# 3. Defectos de captura por fila
# --------------------------------------------------------------------------- #
def aplicar_defectos(U, K, rng):
    N = len(K["prop"])
    p = K["prop"]
    d = {}
    # m²
    m2 = U["m2"][p].copy()
    m2 = np.where(rng.random(N) < C.P_M2_INFLADO, np.round(m2 * rng.uniform(1.25, 2.0, N)), m2)
    m2 = np.where(rng.random(N) < C.P_M2_FALTANTE, np.nan, m2)
    d["m2"] = m2
    d["m2_terreno"] = np.where(rng.random(N) < 0.15, np.nan, U["m2_terreno"][p])
    d["rec"] = np.where(rng.random(N) < C.P_REC_FALTANTE, np.nan, U["rec"][p])
    d["banos"] = np.where(rng.random(N) < C.P_BANOS_FALTANTE, np.nan, U["banos"][p])
    d["estac"] = np.where(rng.random(N) < C.P_ESTAC_FALTANTE, np.nan, U["estac"][p])

    # precio (tendencia suave +0.4% semanal + multiplicador del duplicado)
    precio = U["precio"][p] * K["mult"] * (1 + 0.004 * K["corr"])
    oper = U["operacion"][p]
    precio = np.where(oper == 0, np.round(precio / 5000) * 5000, np.round(precio / 100) * 100)
    # renta capturada como venta
    renta_en_venta = (oper == 0) & (rng.random(N) < C.P_RENTA_COMO_VENTA)
    precio = np.where(renta_en_venta, np.round(precio * 0.005 / 100) * 100, precio)
    precio = np.where(rng.random(N) < C.P_PRECIO_FALTANTE, np.nan, precio)
    # USD en venta alta de Monterrey / San Pedro
    alto = (oper == 0) & (precio > 6_000_000) & np.isin(U["muni"][p], [0, 1]) & (rng.random(N) < 0.15)
    moneda_usd = alto
    precio = np.where(moneda_usd, np.round(precio / C.FX_USD_MXN, -2), precio)
    d["precio"] = precio
    d["usd"] = moneda_usd

    # texto de colonia y municipio (variantes de escritura)
    nombres = U["col_nom"][U["col"][p]]
    v = rng.choice(8, N, p=[0.66, 0.06, 0.05, 0.08, 0.05, 0.02, 0.04, 0.04])
    pos = rng.integers(1, 50, N)
    out = np.empty(N, dtype=object)
    for i in range(N):
        nom = nombres[i]; vi = v[i]
        if vi == 0: out[i] = nom
        elif vi == 1: out[i] = nom.upper()
        elif vi == 2: out[i] = nom.lower()
        elif vi == 3: out[i] = quitar_acentos(nom)
        elif vi == 4: out[i] = "Col. " + nom
        elif vi == 5: out[i] = "  " + nom.replace(" ", "  ") + " "
        elif vi == 6:
            k = pos[i] % max(len(nom) - 2, 1) + 1
            out[i] = nom[:k] + nom[k + 1:]          # typo: falta una letra
        else: out[i] = None
    d["colonia_txt"] = out

    mnames = U["muni_names"]
    vm = rng.choice(4, N, p=[0.74, 0.20, 0.04, 0.02])
    ma = rng.integers(0, 10, N)
    outm = np.empty(N, dtype=object)
    for i in range(N):
        mn = mnames[U["muni"][p[i]]]
        if vm[i] == 0: outm[i] = mn
        elif vm[i] == 1:
            al = C.MUNICIPIOS[mn]["aliases"]; outm[i] = al[ma[i] % len(al)]
        elif vm[i] == 2: outm[i] = mn.upper()
        else: outm[i] = None
    d["municipio_txt"] = outm

    # fecha de publicación: 0-20 días antes de la corrida
    d["dias_atras"] = rng.integers(0, 21, N)
    d["formato_precio"] = rng.random(N)
    return d


# --------------------------------------------------------------------------- #
# 4. Formateo por portal (cada uno distinto) y escritura
# --------------------------------------------------------------------------- #
def _txt_precio_i24(x, usd, venta, u):
    if x != x: return None
    if usd: return f"USD {int(x):,}"
    if venta and x >= 1e6 and u < 0.25: return f"${x / 1e6:.2f} MDP"
    return f"${int(x):,} MXN"


def _txt_precio_viva(x, usd, u):
    if x != x: return None
    if usd: return f"USD {int(x)}"
    return f"{int(x):,}" if u < 0.5 else str(int(x))


def construir_registros(portal, idx, U, K, D, fecha_corrida):
    p = K["prop"][idx]
    oper = U["operacion"][p]; tipo = U["tipo"][p]
    m2 = lista_int(D["m2"][idx]); m2t = lista_int(D["m2_terreno"][idx])
    rec = lista_int(D["rec"][idx]); ban = lista_int(D["banos"][idx]); est = lista_int(D["estac"][idx])
    precio = D["precio"][idx]; usd = D["usd"][idx]; fp = D["formato_precio"][idx]
    col = D["colonia_txt"][idx]; mun = D["municipio_txt"][idx]
    lids = K["lid"][idx]
    d0 = date.fromisoformat(fecha_corrida)
    fechas = [(d0 - timedelta(days=int(k))).isoformat() for k in D["dias_atras"][idx]]
    pre = {"inmuebles24": "I24-", "lamudi": "LMD-", "vivanuncios": "VVA-", "mercadolibre": "MLM"}[portal]
    ids = [f"{pre}{10_000_000 + int(x)}" for x in lids]
    t_es = [["Casa", "Departamento", "Terreno"][t] for t in tipo]
    recs = []

    if portal == "inmuebles24":
        for i in range(len(idx)):
            v = oper[i] == 0
            recs.append({
                "id": ids[i], "titulo": f"{t_es[i]} en {'venta' if v else 'renta'} en {col[i] or ''}".strip(),
                "precio": _txt_precio_i24(precio[i], usd[i], v, fp[i]),
                "tipo_operacion": "Venta" if v else "Renta", "tipo_propiedad": t_es[i],
                "m2_construccion": None if tipo[i] == 2 else m2[i], "m2_terreno": m2[i] if tipo[i] == 2 else m2t[i],
                "recamaras": rec[i], "banos": ban[i], "estacionamientos": est[i],
                "municipio": mun[i], "colonia": col[i], "fecha_publicacion": fechas[i]})
    elif portal == "lamudi":
        te = ["house", "apartment", "land"]
        for i in range(len(idx)):
            recs.append({
                "listing_id": ids[i], "title": f"{t_es[i]} - {col[i] or 'AMM'}",
                "price": None if precio[i] != precio[i] else int(precio[i]), "currency": "USD" if usd[i] else "MXN",
                "operation": "sale" if oper[i] == 0 else "rent", "property_type": te[tipo[i]],
                "built_area": None if tipo[i] == 2 else m2[i], "land_area": m2[i] if tipo[i] == 2 else m2t[i],
                "bedrooms": rec[i], "bathrooms": ban[i], "parking": est[i],
                "city": mun[i], "neighborhood": col[i], "published_at": fechas[i]})
    elif portal == "vivanuncios":
        for i in range(len(idx)):
            if col[i] and mun[i]: zona = f"{col[i]}, {mun[i]}"
            else: zona = col[i] or mun[i]
            recs.append({
                "aid": ids[i], "titulo": f"{t_es[i]} {'en venta' if oper[i] == 0 else 'en alquiler'}",
                "precio_txt": _txt_precio_viva(precio[i], usd[i], fp[i]),
                "operacion": "venta" if oper[i] == 0 else "alquiler",
                "categoria": ["casas", "departamentos", "terrenos"][tipo[i]],
                "superficie": None if m2[i] is None else f"{m2[i]} m²",
                "habitaciones": rec[i], "banos": ban[i], "cochera": est[i],
                "zona": zona, "fecha": fechas[i]})
    else:  # mercadolibre
        te = ["house", "apartment", "land"]
        for i in range(len(idx)):
            areas = {"TOTAL_AREA": m2[i]} if tipo[i] == 2 else {"COVERED_AREA": m2[i], "TOTAL_AREA": m2t[i]}
            recs.append({
                "id": ids[i], "title": f"{t_es[i]} {col[i] or ''}".strip(),
                "price": {"amount": None if precio[i] != precio[i] else int(precio[i]),
                          "currency_id": "USD" if usd[i] else "MXN"},
                "listing_type": "sale" if oper[i] == 0 else "rental", "category": te[tipo[i]],
                "atributos": {**areas, "BEDROOMS": rec[i], "FULL_BATHROOMS": ban[i], "PARKING_LOTS": est[i]},
                "location": {"city": mun[i], "neighborhood": col[i]}, "date_created": fechas[i]})
    return recs


def escribir_partes(recs, carpeta, tam_parte=150_000):
    carpeta.mkdir(parents=True, exist_ok=True)
    for k in range(0, len(recs), tam_parte):
        ruta = carpeta / f"part-{k // tam_parte + 1:04d}.jsonl.gz"
        with gzip.open(ruta, "wt", encoding="utf-8", compresslevel=3) as f:
            for r in recs[k:k + tam_parte]:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")


def main():
    t0 = time.time()
    C.asegurar_carpetas()
    rng = np.random.default_rng(C.SEMILLA)
    if C.BRONCE.exists():
        shutil.rmtree(C.BRONCE)

    factor = 1 + C.P_RECAPTURA * (len(C.CORRIDAS) - 1) / len(C.CORRIDAS) + C.P_DUP_INTERPORTAL
    n_unicos = int(C.N_REGISTROS_CRUDOS / factor)
    print(f"[ingesta] objetivo {C.N_REGISTROS_CRUDOS:,} registros crudos -> {n_unicos:,} inmuebles únicos")
    U = generar_universo(n_unicos, rng)
    K = generar_capturas(U, rng)
    D = aplicar_defectos(U, K, rng)
    total = 0
    nombres = list(C.PORTALES)
    for pi, portal in enumerate(nombres):
        for ci, fecha in enumerate(C.CORRIDAS):
            idx = np.where((K["port"] == pi) & (K["corr"] == ci))[0]
            recs = construir_registros(portal, idx, U, K, D, fecha)
            escribir_partes(recs, C.BRONCE / portal / f"corrida={fecha}")
            total += len(recs)
            print(f"[ingesta] {portal:13s} {fecha}  {len(recs):>8,} registros")
    seg = time.time() - t0
    meta = {"registros_crudos": total, "inmuebles_unicos_simulados": n_unicos, "segundos": round(seg, 1),
            "nota": "Dataset SIMULADO con fines académicos"}
    (C.METRICAS / "etapa1_ingesta.json").write_text(json.dumps(meta, indent=2, ensure_ascii=False))
    print(f"[ingesta] listo: {total:,} registros crudos en {seg:.0f} s")


if __name__ == "__main__":
    main()
