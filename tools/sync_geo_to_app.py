#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Copia el modulo geografico y la dimension al repo de la app Streamlit.

    python tools/sync_geo_to_app.py [ruta_al_repo_de_la_app]

Los dos repos son independientes y no comparten paquete instalable, asi que
la unica forma de que ETL y app resuelvan la geografia igual es copiar la
fuente. Este script existe justamente para que la copia sea explicita y
repetible: la causa original del problema fue que la app mantenia SU PROPIO
diccionario de municipios (src/utils/geo_utils.py) que se desincronizo del
catalogo del ETL.

Los archivos copiados quedan marcados como generados. No editarlos en el
repo de la app: editar aqui y volver a sincronizar.
"""

import io
import os
import shutil
import sys

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_APP_REPO = os.path.normpath(
    os.path.join(REPO_ROOT, "..", "Real_State_Analyst")
)

MODULES = [
    ("src/geo/__init__.py", "src/geo/__init__.py"),
    ("src/geo/normalize.py", "src/geo/normalize.py"),
    ("src/geo/aliases.py", "src/geo/aliases.py"),
    ("src/geo/divipola.py", "src/geo/divipola.py"),
    ("src/geo/geo_resolver.py", "src/geo/geo_resolver.py"),
    ("src/quality/__init__.py", "src/quality/__init__.py"),
    ("src/quality/listing_lifecycle.py", "src/quality/listing_lifecycle.py"),
]

DIMENSION_FILES = [
    "dim_departamento.parquet",
    "dim_municipio.parquet",
    "dim_municipio_alias.parquet",
    "dim_barrio.parquet",
]

BANNER = (
    "# === ARCHIVO GENERADO — NO EDITAR AQUI ===\n"
    "# Origen: lakehouse-repo/{source}\n"
    "# Regenerar: python tools/sync_geo_to_app.py\n"
)


def main():
    app_repo = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_APP_REPO
    if not os.path.isdir(app_repo):
        print(f"No existe el repo de la app: {app_repo}")
        return 1

    print(f"Sincronizando hacia {app_repo}")
    for source_rel, target_rel in MODULES:
        source = os.path.join(REPO_ROOT, source_rel)
        target = os.path.join(app_repo, target_rel)
        if not os.path.isfile(source):
            print(f"  FALTA {source_rel}")
            continue
        os.makedirs(os.path.dirname(target), exist_ok=True)
        content = io.open(source, encoding="utf-8").read()
        io.open(target, "w", encoding="utf-8").write(
            BANNER.format(source=source_rel) + content
        )
        print(f"  {source_rel} -> {target_rel}")

    source_dim = os.path.join(REPO_ROOT, "reference", "geo")
    target_dim = os.path.join(app_repo, "reference", "geo")
    if os.path.isdir(source_dim):
        os.makedirs(target_dim, exist_ok=True)
        for name in DIMENSION_FILES:
            source_file = os.path.join(source_dim, name)
            if os.path.isfile(source_file):
                shutil.copy(source_file, os.path.join(target_dim, name))
                size_kb = os.path.getsize(source_file) / 1024
                print(f"  reference/geo/{name} ({size_kb:.0f} KB)")
    else:
        print("  AVISO: falta reference/geo/. Corre tools/build_geo_dimension.py primero.")

    print("\nListo.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
