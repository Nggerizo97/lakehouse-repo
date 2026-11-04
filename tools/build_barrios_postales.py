#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Catalogo oficial barrio -> codigo postal (4-72) para toda Colombia.

    python tools/build_barrios_postales.py                    # descarga de datos.gov.co
    python tools/build_barrios_postales.py --source raw.json  # desde un JSON ya bajado

Fuente: "Codigos Postales Nacionales", Servicios Postales Nacionales 4-72,
publicado en datos.gov.co (dataset ixig-z8b5) con licencia CC BY-SA 4.0. Cada
fila es un codigo postal con la lista de barrios y veredas que contiene.

Por que esta fuente:
- reference/barrios_seed.py tiene 192 barrios urbanos escritos a mano (34 en
  Bogota, 36 en Medellin, 9 en Barranquilla) y mezcla localidades y comunas
  con barrios. 4-72 trae ~900 en Bogota, 263 en Medellin, 158 en Barranquilla.
- El codigo postal agrupa barrios vecinos (Medellin: 41 codigos), asi que da
  un nivel intermedio estandar entre barrio y municipio.
- codigospostalesdelmundo.com reproduce este mismo dataset, pero sus terminos
  prohiben copiar su contenido sin autorizacion escrita.

Limitaciones conocidas del dataset publicado:
- Los codigos llegan como numeros con separador de miles y sin ceros:
  "153.42" es 153420, "55.03" es 055030. Se reconstruyen y se verifica que
  empiecen por el codigo del departamento.
- El texto de barrios y de veredas esta cortado a 255 caracteres (pasa en
  ~200 de los ~560 codigos con barrios, sobre todo en ciudades grandes). Los
  barrios que quedaban despues del corte no estan, y el ultimo elemento de
  una lista cortada queda partido ("Urbaniza"), asi que se descarta y la
  fila se marca con lista_truncada=True.

Escribe reference/barrios_postales_4_72.csv. Licencia CC BY-SA 4.0: si ese
archivo se redistribuye fuera del proyecto debe conservar la atribucion a 4-72
y la misma licencia.
"""

import argparse
import json
import os
import re
import sys
import urllib.request

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from src.geo.normalize import normalize_text  # noqa: E402

DATASET_URL = "https://www.datos.gov.co/resource/ixig-z8b5.json?$limit=50000"
FUENTE = "4-72 datos.gov.co ixig-z8b5 CC-BY-SA-4.0"
DEFAULT_OUT = os.path.join(REPO_ROOT, "reference", "barrios_postales_4_72.csv")
MUNICIPIOS_XLSX = os.path.join(REPO_ROOT, "reference", "mapeo_geografico_colombia.xlsx")

# datos.gov.co corta los campos de texto largos a este numero de caracteres.
LIMITE_CAMPO = 255

_SIN_INFO = re.compile(r"^\s*sin informaci", re.IGNORECASE)
# Separador entre elementos: guion seguido de espacio. Un guion pegado es parte
# del nombre ("El Nogal-Los Almendros", "Cooperativa de Sub-Oficiales").
_SEPARADOR = re.compile(r"\s*-\s+")
_PARENTESIS = re.compile(r"\([^)]*\)")
_ARTICULO = re.compile(r"^(el|la|los|las)\s+")
_PREFIJO = re.compile(r"^(barrio|urbanizacion|urb|conjunto|sector)\s+")
# Solo numeraciones de etapa: "Norte", "Sur", "Central" distinguen barrios
# distintos ("Chapinero Norte" y "Chapinero Central") y se conservan.
_SUFIJO = re.compile(r"\s+(no\s*\d{1,2}|\d{1,2}|i{1,3}|iv|etapa|sector)$")


def reconstruir_codigo_postal(valor) -> str:
    """'153.42' -> '153420', '55.03' -> '055030', '880.007' -> '880007'."""
    entero, _, miles = str(valor).strip().partition(".")
    digitos = entero + miles.ljust(3, "0") if miles else entero
    return digitos.zfill(6)


def codigo_municipio(valor) -> str:
    """'5001' -> '05001'. El dataset publica el codigo DANE sin cero inicial."""
    return str(valor).strip().split(".")[0].zfill(5)


def separar_lista(texto):
    """Lista de nombres y si el campo llego cortado por el limite de 255."""
    if texto is None or _SIN_INFO.match(str(texto)):
        return [], False
    texto = str(texto)
    truncada = len(texto) >= LIMITE_CAMPO - 1
    partes = [p.strip(" -") for p in _SEPARADOR.split(texto)]
    if truncada and partes:
        partes = partes[:-1]  # el ultimo quedo partido por el corte
    # Se descartan restos sin letras ("442") y fragmentos de menos de 3 caracteres.
    return [p for p in partes if len(p) >= 3 and re.search(r"[^\W\d_]", p)], truncada


def limpiar_nombre(nombre: str) -> str:
    nombre = re.sub(r"\s+", " ", nombre).strip()
    return nombre.title() if nombre.isupper() else nombre


def clave_base(nombre: str) -> str:
    """Clave tolerante a las variantes con que los portales escriben el barrio.

    'Los Balsos No.2' -> 'balsos', 'El Chicó' -> 'chico',
    'Belén (Antes Provivienda I Etapa)' -> 'belen'. Sirve para emparejar con
    el texto de los avisos; el nombre oficial se conserva en `nombre`.
    """
    clave = normalize_text(_PARENTESIS.sub(" ", nombre))
    clave = _PREFIJO.sub("", clave)
    anterior = None
    while anterior != clave:
        anterior = clave
        clave = _SUFIJO.sub("", clave).strip()
    return _ARTICULO.sub("", clave).strip() or normalize_text(nombre)


def construir(registros, municipios: pd.DataFrame = None) -> pd.DataFrame:
    """Una fila por (municipio, nivel, barrio) con sus codigos postales."""
    filas = []
    for reg in registros:
        cod_mpio = codigo_municipio(reg["codigo_municipio"])
        cp = reconstruir_codigo_postal(reg["codigo_postal"])
        cod_dpto = cod_mpio[:2]
        if not cp.startswith(cod_dpto):
            raise ValueError(f"Codigo postal {cp} no corresponde al departamento {cod_dpto}: {reg}")
        for nivel, campo in (("barrio", "barrios_contenidos_en_el"), ("vereda", "veredas_contenidas_en_el")):
            nombres, truncada = separar_lista(reg.get(campo))
            for nombre in nombres:
                nombre = limpiar_nombre(nombre)
                filas.append({
                    "cod_dpto": cod_dpto,
                    "cod_mpio": cod_mpio,
                    "municipio_4_72": str(reg.get("nombre_municipio", "")).strip(),
                    "nivel": nivel,
                    "nombre": nombre,
                    "key_barrio": normalize_text(nombre),
                    "key_base": clave_base(nombre),
                    "codigo_postal": cp,
                    "tipo_zona": reg.get("tipo"),
                    "lista_truncada": truncada,
                })
    df = pd.DataFrame(filas)
    if df.empty:
        return df

    # Un barrio puede aparecer en dos codigos ("Calle Nueva" en 050014 y 050015).
    df = (
        df.sort_values(["cod_mpio", "nivel", "key_barrio", "codigo_postal"])
        .groupby(["cod_mpio", "nivel", "key_barrio"], as_index=False)
        .agg(
            cod_dpto=("cod_dpto", "first"),
            municipio_4_72=("municipio_4_72", "first"),
            nombre=("nombre", "first"),
            key_base=("key_base", "first"),
            codigo_postal=("codigo_postal", "first"),
            codigos_postales=("codigo_postal", lambda s: "|".join(sorted(set(s)))),
            tipo_zona=("tipo_zona", "first"),
            lista_truncada=("lista_truncada", "max"),
        )
    )
    if municipios is not None:
        df = df.merge(municipios[["cod_mpio", "departamento", "municipio"]], on="cod_mpio", how="left")
    else:
        df["departamento"], df["municipio"] = None, df["municipio_4_72"]
    df["municipio"] = df["municipio"].fillna(df["municipio_4_72"])
    df["fuente"] = FUENTE
    return df[[
        "cod_dpto", "departamento", "cod_mpio", "municipio", "nivel", "nombre", "key_barrio",
        "key_base", "codigo_postal", "codigos_postales", "tipo_zona", "lista_truncada", "fuente",
    ]].reset_index(drop=True)


def _descargar():
    request = urllib.request.Request(DATASET_URL, headers={"User-Agent": "lakehouse-repo/build_barrios_postales"})
    with urllib.request.urlopen(request, timeout=120) as response:
        return json.loads(response.read().decode("utf-8"))


def _municipios_oficiales():
    """Nombres DIVIPOLA del repo, para que el catalogo use los mismos que dim_municipio."""
    if not os.path.isfile(MUNICIPIOS_XLSX):
        return None
    df = pd.read_excel(MUNICIPIOS_XLSX, "municipios", dtype={"cod_mpio": str})
    df["cod_mpio"] = df["cod_mpio"].str.zfill(5)
    return df[["cod_mpio", "departamento", "municipio"]]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", help="JSON ya descargado del dataset (por defecto se descarga)")
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    if args.source:
        with open(args.source, encoding="utf-8") as handle:
            registros = json.load(handle)
    else:
        print(f"Descargando {DATASET_URL} ...")
        registros = _descargar()
    print(f"  {len(registros):,} codigos postales")

    df = construir(registros, _municipios_oficiales())
    df.to_csv(args.out, index=False)

    barrios = df[df["nivel"] == "barrio"]
    print(f"  {len(barrios):,} barrios en {barrios['cod_mpio'].nunique()} municipios | "
          f"{(df['nivel'] == 'vereda').sum():,} veredas")
    print(f"  barrios de listas cortadas a {LIMITE_CAMPO} caracteres: "
          f"{barrios['lista_truncada'].mean() * 100:.1f}%")
    top = barrios.groupby("municipio").size().sort_values(ascending=False).head(10)
    print("  por municipio:", ", ".join(f"{m} {n}" for m, n in top.items()))
    print(f"-> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
