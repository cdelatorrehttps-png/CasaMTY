"""
(OPCIONAL) Carga una muestra de los documentos crudos de la capa bronce a MongoDB.

La Fase 2 definió MongoDB Community como almacén de documentos con campos irregulares (cada
portal manda campos distintos). En el MVP el crudo vive como JSONL.gz en el Data Lake; este script
demuestra la carga a MongoDB si lo tienes instalado:

    pip install pymongo
    python src/opcional_mongodb.py                       # mongodb://localhost:27017, 50,000 docs
    python src/opcional_mongodb.py --uri mongodb://localhost:27017 --max 100000

Colección: casamty.anuncios_crudos  (campos añadidos: _portal, _corrida)
"""
import argparse
import gzip
import json

import config as C


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--uri", default="mongodb://localhost:27017")
    ap.add_argument("--max", type=int, default=50_000, help="máximo de documentos por portal y corrida")
    args = ap.parse_args()
    try:
        from pymongo import MongoClient
    except ImportError:
        raise SystemExit("Falta pymongo: pip install pymongo")
    col = MongoClient(args.uri, serverSelectionTimeoutMS=3000)["casamty"]["anuncios_crudos"]
    col.delete_many({})
    total = 0
    for portal_dir in sorted(C.BRONCE.iterdir()):
        for corrida_dir in sorted(portal_dir.glob("corrida=*")):
            lote, cuenta = [], 0
            for parte in sorted(corrida_dir.glob("*.jsonl.gz")):
                with gzip.open(parte, "rt", encoding="utf-8") as f:
                    for linea in f:
                        d = json.loads(linea)
                        d["_portal"] = portal_dir.name
                        d["_corrida"] = corrida_dir.name.split("=")[1]
                        lote.append(d); cuenta += 1
                        if len(lote) >= 5000:
                            col.insert_many(lote); lote = []
                        if cuenta >= args.max:
                            break
                if cuenta >= args.max:
                    break
            if lote:
                col.insert_many(lote)
            total += cuenta
    col.create_index([("_portal", 1), ("_corrida", 1)])
    print(f"[mongodb] {total:,} documentos cargados en casamty.anuncios_crudos")


if __name__ == "__main__":
    main()
