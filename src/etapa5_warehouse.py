"""
ETAPA 5 - DATA WAREHOUSE ESCOLAR (modelo estrella)

Por defecto carga en SQLite (cero instalación, verificable en cualquier PC):
    python src/etapa5_warehouse.py
Para usar PostgreSQL (el warehouse definido en la Fase 2) se pasa una URL de SQLAlchemy:
    pip install sqlalchemy psycopg2-binary
    python src/etapa5_warehouse.py --url postgresql+psycopg2://usuario:clave@localhost:5432/casamty
(El DDL equivalente para PostgreSQL está en sql/esquema_postgresql.sql.)

Tablas: dim_ubicacion, dim_inmueble_tipo, dim_operacion, dim_fuente, hecho_anuncio, hecho_estimacion.
Mide además el "costo por consulta analítica" (tiempo promedio de 5 consultas típicas).
"""
import argparse
import json
import sqlite3
import time

import pyarrow.dataset as ds

import config as C

CONSULTAS = {
    "precio_m2_promedio_por_municipio_casas_venta": """
        SELECT u.municipio, COUNT(*) AS n, AVG(h.precio_m2) AS ppm2_promedio
        FROM hecho_anuncio h JOIN dim_ubicacion u USING(id_ubicacion)
        WHERE h.flag_usable = 1 AND h.id_operacion = 1 AND h.id_tipo = 1
        GROUP BY u.municipio ORDER BY ppm2_promedio DESC""",
    "inventario_por_operacion_y_tipo": """
        SELECT o.operacion, t.tipo, COUNT(*) AS anuncios
        FROM hecho_anuncio h JOIN dim_operacion o USING(id_operacion) JOIN dim_inmueble_tipo t USING(id_tipo)
        WHERE h.flag_duplicado = 0 GROUP BY o.operacion, t.tipo""",
    "tasa_duplicados_por_fuente": """
        SELECT f.fuente, ROUND(100.0 * SUM(h.flag_duplicado) / COUNT(*), 2) AS pct_duplicados
        FROM hecho_anuncio h JOIN dim_fuente f USING(id_fuente) GROUP BY f.fuente""",
    "top10_colonias_mas_caras_casas_venta": """
        SELECT u.municipio, u.colonia, e.n_anuncios_usados, e.precio_m2_mediana_observada
        FROM hecho_estimacion e JOIN dim_ubicacion u USING(id_ubicacion)
        WHERE e.id_tipo = 1 AND e.id_operacion = 1 AND e.suficiente_muestra = 1
        ORDER BY e.precio_m2_mediana_observada DESC LIMIT 10""",
    "anuncios_sobre_precio_estimado_san_pedro": """
        SELECT u.colonia, COUNT(*) AS anuncios, ROUND(AVG(h.precio_m2), 0) AS ppm2_publicado,
               ROUND(AVG(e.precio_m2_estimado_modelo), 0) AS ppm2_estimado
        FROM hecho_anuncio h JOIN dim_ubicacion u USING(id_ubicacion)
        JOIN hecho_estimacion e ON e.id_ubicacion = h.id_ubicacion AND e.id_tipo = h.id_tipo AND e.id_operacion = h.id_operacion
        WHERE h.flag_usable = 1 AND u.municipio = 'San Pedro Garza García' AND h.id_operacion = 1
        GROUP BY u.colonia ORDER BY ppm2_publicado DESC""",
}


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=None, help="URL SQLAlchemy (ej. postgresql+psycopg2://...). Por defecto SQLite local.")
    args = ap.parse_args()
    t0 = time.time()
    C.asegurar_carpetas()

    if args.url:
        from sqlalchemy import create_engine
        con = create_engine(args.url)
        destino = args.url.split("@")[-1]
    else:
        if C.WAREHOUSE_SQLITE.exists():
            C.WAREHOUSE_SQLITE.unlink()
        con = sqlite3.connect(C.WAREHOUSE_SQLITE)
        destino = str(C.WAREHOUSE_SQLITE)

    import pandas as pd
    for nombre in ["dim_ubicacion", "dim_inmueble_tipo", "dim_operacion", "dim_fuente"]:
        pd.read_parquet(C.ORO / f"{nombre}.parquet").to_sql(nombre, con, if_exists="replace", index=False)
    est = pd.read_parquet(C.ORO / "hecho_estimacion.parquet")
    for c in ["suficiente_muestra", "cumple_meta_mape"]:
        est[c] = est[c].astype(int)
    est.to_sql("hecho_estimacion", con, if_exists="replace", index=False)

    dataset = ds.dataset(str(C.ORO / "hecho_anuncio"), format="parquet")
    n = 0
    primero = True
    for lote in dataset.to_batches(batch_size=200_000):
        df = lote.to_pandas()
        for c in ["flag_completo", "flag_duplicado", "flag_outlier", "flag_usable"]:
            df[c] = df[c].astype(int)
        for c in ["fecha_captura", "fecha_publicacion"]:
            df[c] = df[c].astype(str)
        df.to_sql("hecho_anuncio", con, if_exists="replace" if primero else "append", index=False, chunksize=50_000)
        primero = False
        n += len(df)
    print(f"[warehouse] hecho_anuncio: {n:,} filas -> {destino}")

    if not args.url:
        cur = con.cursor()
        for sql in ["CREATE INDEX ix_h_ub ON hecho_anuncio(id_ubicacion)",
                    "CREATE INDEX ix_h_op_tipo ON hecho_anuncio(id_operacion, id_tipo)",
                    "CREATE INDEX ix_h_usable ON hecho_anuncio(flag_usable)"]:
            cur.execute(sql)
        con.commit()
        tiempos = {}
        for nombre, sql in CONSULTAS.items():
            ts = []
            for _ in range(3):
                t1 = time.time(); cur.execute(sql).fetchall(); ts.append(time.time() - t1)
            tiempos[nombre] = round(min(ts), 3)
        con.close()
        out = {"filas_hecho_anuncio": n, "segundos_consultas": tiempos,
               "segundos_promedio_por_consulta": round(sum(tiempos.values()) / len(tiempos), 3),
               "costo_por_consulta_usd": 0.0, "tamano_sqlite_mb": round(C.WAREHOUSE_SQLITE.stat().st_size / 1e6, 1),
               "segundos_etapa": round(time.time() - t0, 1)}
        (C.METRICAS / "etapa5_warehouse.json").write_text(json.dumps(out, indent=2, ensure_ascii=False))
        print(json.dumps(out, indent=2, ensure_ascii=False))
    else:
        print("[warehouse] carga a PostgreSQL terminada (crea los índices con sql/esquema_postgresql.sql).")


if __name__ == "__main__":
    main()
