"""
CasaMTY - Orquestador escolar del pipeline (reemplaza a Airflow, ver Fase 2).

Uso:
    python correr_pipeline.py                 # pipeline completo (1.5 millones de registros)
    python correr_pipeline.py --n 100000      # prueba rápida
    python correr_pipeline.py --desde 2       # reanuda desde la etapa 2 (ya existe el crudo)

Se puede programar con el Programador de tareas de Windows o con cron en Linux.
Guarda el tiempo de cada etapa en data/metricas/tiempos_etapas.json.
"""
import argparse
import json
import os
import subprocess
import sys
import time
from pathlib import Path

RAIZ = Path(__file__).resolve().parent
ETAPAS = [
    ("etapa1", "Ingesta batch -> Data Lake bronce", "etapa1_ingesta_bronce.py"),
    ("etapa2", "Limpieza con Spark -> capa plata", "etapa2_plata_spark.py"),
    ("etapa3", "Modelo estrella + agregados -> capa oro", "etapa3_oro_spark.py"),
    ("etapa4", "Modelo de estimación de precio", "etapa4_modelo.py"),
    ("etapa5", "Carga al warehouse (SQLite)", "etapa5_warehouse.py"),
    ("etapa6", "Dashboard y gráficas", "etapa6_dashboard.py"),
    ("etapa7", "Reporte técnico", "etapa7_reporte.py"),
]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=None, help="registros crudos a generar (por defecto 1,500,000)")
    ap.add_argument("--desde", type=int, default=1, help="número de etapa desde la que se corre (1-7)")
    args = ap.parse_args()
    env = dict(os.environ)
    if args.n:
        env["CASAMTY_N"] = str(args.n)
    (RAIZ / "data" / "metricas").mkdir(parents=True, exist_ok=True)
    ruta_t = RAIZ / "data" / "metricas" / "tiempos_etapas.json"
    tiempos = json.loads(ruta_t.read_text()) if (args.desde > 1 and ruta_t.exists()) else {}
    t_total = time.time()
    for i, (clave, nombre, script) in enumerate(ETAPAS, start=1):
        if i < args.desde:
            continue
        print(f"\n===== Etapa {i}/{len(ETAPAS)}: {nombre} =====", flush=True)
        t0 = time.time()
        r = subprocess.run([sys.executable, str(RAIZ / "src" / script)], env=env, cwd=RAIZ)
        if r.returncode != 0:
            print(f"ERROR en la etapa {i} ({script}). Pipeline detenido.")
            sys.exit(r.returncode)
        tiempos[clave] = round(time.time() - t0, 1)
        ruta_t.write_text(json.dumps(tiempos, indent=2))
        print(f"----- etapa {i} lista en {tiempos[clave]} s", flush=True)
    print(f"\nPipeline terminado en {(time.time() - t_total) / 60:.1f} min. Abre dashboard/index.html")


if __name__ == "__main__":
    main()
