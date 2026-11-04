#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Catalogo oficial de barrios y veredas de Colombia con codigo postal (4-72).

    pip install shapely                                   # solo para esta herramienta
    python tools/build_barrios_postales.py                # descarga todo de 4-72
    python tools/build_barrios_postales.py --cache DIR    # guarda y reusa las descargas

Todas las fuentes son de Servicios Postales Nacionales 4-72:

1. Capas Barrio y Vereda del visor oficial de codigo postal
   (visor.codigopostal.gov.co, servicio ArcGIS DivisionAdministrativa), con
   poligono: ~11.150 barrios en 174 municipios y ~15.400 veredas en ~440.
2. Capa CodigoPostal del mismo visor: cada barrio y vereda recibe el codigo
   postal del poligono que contiene su punto representativo. Contra la lista
   publicada en datos.gov.co el codigo coincide en el 98,7 % de los barrios.
3. "Codigos Postales Nacionales" en datos.gov.co (ixig-z8b5, CC BY-SA 4.0):
   lista de barrios y veredas por codigo postal. Aporta 13 municipios que la
   capa no tiene, barrios sueltos que faltan en la capa y, en Bogota, el
   nombre de la localidad de cada codigo postal. Sus listas estan cortadas a
   255 caracteres, por eso es fuente complementaria y no principal.

Por que no la semilla manual ni codigospostalesdelmundo.com:
reference/barrios_seed.py tiene 192 barrios escritos a mano y mezcla
localidades y comunas con barrios; el sitio reproduce la fuente 3 (con el
mismo corte) y sus terminos prohiben copiar su contenido sin autorizacion.

Escribe reference/barrios_postales_4_72.csv, una fila por (municipio, nivel,
nombre). Licencia de la fuente 3: CC BY-SA 4.0; si el archivo se redistribuye
fuera del proyecto debe conservar la atribucion a 4-72 y la misma licencia.
"""

import argparse
import json
import os
import re
import sys
import time
import urllib.parse
import urllib.request

import pandas as pd

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, REPO_ROOT)

from src.geo.normalize import normalize_text  # noqa: E402

DATASET_URL = "https://www.datos.gov.co/resource/ixig-z8b5.json?$limit=50000"
ARCGIS = "https://visor.codigopostal.gov.co/arcgis/rest/services/"
CAPA_MUNICIPIO = "DivisionAdministrativa/MapServer/3"
CAPA_BARRIO = "DivisionAdministrativa/MapServer/1"
CAPA_VEREDA = "DivisionAdministrativa/MapServer/2"
CAPA_CODIGO_POSTAL = "Division_Codigo_postal/MapServer/1"
USER_AGENT = "lakehouse-repo/build_barrios_postales"

FUENTE_CAPA = "4-72 visor codigo postal (capa)"
FUENTE_LISTA = "4-72 datos.gov.co ixig-z8b5 CC-BY-SA-4.0 (lista)"
DEFAULT_OUT = os.path.join(REPO_ROOT, "reference", "barrios_postales_4_72.csv")
MUNICIPIOS_XLSX = os.path.join(REPO_ROOT, "reference", "mapeo_geografico_colombia.xlsx")

# datos.gov.co corta los campos de texto largos a este numero de caracteres.
LIMITE_CAMPO = 255

# Nombres de relleno de las fuentes, no son barrios ni veredas.
NOMBRES_GENERICOS = {
    "sin definir", "sin informacion", "sin nombre", "por definir", "cabecera municipal",
    "zona urbana", "zona rural", "area urbana", "area rural", "casco urbano", "otros barrios",
}

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

COLUMNAS = [
    "cod_dpto", "departamento", "cod_mpio", "municipio", "nivel", "nombre", "key_barrio",
    "key_base", "codigo_postal", "codigos_postales", "localidad", "comuna_id_4_72", "lat", "lon",
    "codigo_4_72", "fuente", "lista_truncada",
]


# ── Normalizacion ─────────────────────────────────────────────────────────

def reconstruir_codigo_postal(valor) -> str:
    """'153.42' -> '153420', '55.03' -> '055030', '880.007' -> '880007'."""
    entero, _, miles = str(valor).strip().partition(".")
    digitos = entero + miles.ljust(3, "0") if miles else entero
    return digitos.zfill(6)


def codigo_municipio(valor) -> str:
    """'5001' -> '05001'. datos.gov.co publica el codigo DANE sin cero inicial."""
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
    nombre = re.sub(r"\s+", " ", str(nombre)).strip()
    return nombre.title() if nombre.isupper() else nombre


def es_generico(nombre: str) -> bool:
    return normalize_text(_PARENTESIS.sub(" ", str(nombre))) in NOMBRES_GENERICOS


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


# ── Fuente 3: listas de datos.gov.co ──────────────────────────────────────

def filas_lista(registros):
    """Filas (municipio, nivel, nombre, codigo postal) de las listas publicadas."""
    filas = []
    for reg in registros:
        cod_mpio = codigo_municipio(reg["codigo_municipio"])
        cp = reconstruir_codigo_postal(reg["codigo_postal"])
        if not cp.startswith(cod_mpio[:2]):
            raise ValueError(f"Codigo postal {cp} no corresponde al departamento {cod_mpio[:2]}: {reg}")
        for nivel, campo in (("barrio", "barrios_contenidos_en_el"), ("vereda", "veredas_contenidas_en_el")):
            nombres, truncada = separar_lista(reg.get(campo))
            for nombre in nombres:
                if es_generico(nombre):
                    continue
                filas.append({"cod_mpio": cod_mpio, "nivel": nivel, "nombre": limpiar_nombre(nombre),
                              "codigo_postal": cp, "fuente": FUENTE_LISTA, "lista_truncada": truncada})
    return pd.DataFrame(filas)


def localidades_bogota(registros) -> dict:
    """codigo postal -> localidad. En Bogota el dataset usa 'LOCALIDAD USAQUEN' como municipio."""
    salida = {}
    for reg in registros:
        nombre = str(reg.get("nombre_municipio", ""))
        if codigo_municipio(reg["codigo_municipio"]) == "11001" and nombre.upper().startswith("LOCALIDAD "):
            salida[reconstruir_codigo_postal(reg["codigo_postal"])] = limpiar_nombre(nombre[10:])
    return salida


# ── Fuentes 1 y 2: capas del visor ────────────────────────────────────────

def _poligono(geometria):
    from shapely.geometry import shape

    if not geometria or not geometria.get("rings"):
        return None
    poligono = shape({"type": "Polygon", "coordinates": geometria["rings"]}).buffer(0)
    return None if poligono.is_empty else poligono


class IndiceCodigoPostal:
    """Punto -> codigo postal, con los poligonos de la capa CodigoPostal."""

    def __init__(self, features):
        from shapely.strtree import STRtree

        pares = [(_poligono(f.get("geometry")), f["attributes"]["Codigo_Postal"]) for f in features]
        pares = [(p, str(c).zfill(6)) for p, c in pares if p is not None and c]
        self.poligonos = [p for p, _ in pares]
        self.codigos = [c for _, c in pares]
        self.arbol = STRtree(self.poligonos)

    def codigo(self, punto):
        for i in self.arbol.query(punto):
            if self.poligonos[i].contains(punto):
                return self.codigos[i]
        return None


def filas_capa(features, nivel, municipio_por_id, indice_cp):
    """Filas de la capa Barrio o Vereda, con punto representativo y codigo postal."""
    filas = []
    for f in features:
        a = f["attributes"]
        nombre = a.get("Nombre")
        if not nombre or es_generico(nombre) or not re.search(r"[^\W\d_]", str(nombre)):
            continue
        poligono = _poligono(f.get("geometry"))
        punto = poligono.representative_point() if poligono is not None else None
        cp = indice_cp.codigo(punto) if punto is not None else None
        # Veredas: Codigo_DANE es el municipio; MunicipioID es un id interno del
        # visor que en ~2.000 veredas viene vacio o no existe en la capa Municipio.
        # Codigo_DANE tambien trae relleno ("0000 "), que se trata como vacio.
        cod_mpio = str(a.get("Codigo_DANE") or "").strip() if nivel == "vereda" else ""
        if not re.fullmatch(r"\d{5}", cod_mpio):
            cod_mpio = municipio_por_id.get(str(a.get("MunicipioID")))
        # Un punto de frontera puede caer en el codigo postal de otro
        # departamento: se descarta en vez de guardar un codigo equivocado.
        if cp and cod_mpio and cp[:2] != cod_mpio[:2]:
            cp = None
        filas.append({
            "cod_mpio": cod_mpio or None,
            "nivel": nivel,
            "nombre": limpiar_nombre(nombre),
            "codigo_postal": cp,
            "comuna_id_4_72": a.get("ComunaID") if a.get("ComunaID") not in (None, "0") else None,
            "lat": round(punto.y, 6) if punto is not None else None,
            "lon": round(punto.x, 6) if punto is not None else None,
            "codigo_4_72": a.get("Codigo"),
            "fuente": FUENTE_CAPA,
            "lista_truncada": False,
        })
    return pd.DataFrame(filas)


# ── Combinacion ───────────────────────────────────────────────────────────

def combinar(capa: pd.DataFrame, lista: pd.DataFrame, municipio_por_cp: dict,
             localidades: dict, municipios: pd.DataFrame = None) -> pd.DataFrame:
    """Una fila por (municipio, nivel, nombre normalizado). La capa manda.

    De la lista se toman los nombres que la capa no tiene y los codigos
    postales adicionales de los que si tiene.
    """
    capa = capa.copy()
    # Sin municipio en la capa: el del codigo postal que contiene el poligono.
    faltan = capa["cod_mpio"].isna()
    capa.loc[faltan, "cod_mpio"] = capa.loc[faltan, "codigo_postal"].map(municipio_por_cp)
    capa = capa[capa["cod_mpio"].notna()]

    df = pd.concat([capa, lista], ignore_index=True)
    df["key_barrio"] = df["nombre"].map(normalize_text)
    df = df[df["key_barrio"].str.len() >= 3]
    df["_orden"] = (df["fuente"] != FUENTE_CAPA).astype(int)
    df = df.sort_values(["cod_mpio", "nivel", "key_barrio", "_orden", "codigo_postal"])

    codigos = (
        df.dropna(subset=["codigo_postal"])
        .groupby(["cod_mpio", "nivel", "key_barrio"])["codigo_postal"]
        .agg(lambda s: "|".join(sorted(set(s))))
        .rename("codigos_postales")
    )
    fuentes = (
        df.groupby(["cod_mpio", "nivel", "key_barrio"])["fuente"]
        .agg(lambda s: " + ".join(sorted(set(s))))
    )
    # Varios poligonos con el mismo nombre en un municipio ("San Pedro" en Bogota):
    # se conserva el primero de la capa y se suman todos sus codigos postales.
    out = df.drop_duplicates(["cod_mpio", "nivel", "key_barrio"]).drop(columns="fuente")
    out = out.join(codigos, on=["cod_mpio", "nivel", "key_barrio"]).join(fuentes, on=["cod_mpio", "nivel", "key_barrio"])
    out["key_base"] = out["nombre"].map(clave_base)
    out["localidad"] = out["codigo_postal"].map(localidades)
    out["cod_dpto"] = out["cod_mpio"].str[:2]
    if municipios is not None:
        out = out.merge(municipios[["cod_mpio", "departamento", "municipio"]], on="cod_mpio", how="left")
    else:
        out["departamento"], out["municipio"] = None, None
    out["lista_truncada"] = out["lista_truncada"].fillna(False).astype(bool)
    return out[COLUMNAS].sort_values(["cod_mpio", "nivel", "key_barrio"]).reset_index(drop=True)


# ── Descargas ─────────────────────────────────────────────────────────────

def _get_json(url):
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    with urllib.request.urlopen(request, timeout=300) as response:
        return json.loads(response.read().decode("utf-8"))


def descargar_capa(capa, geometria=False, tolerancia=0.00001, lote=1000):
    """Todos los registros de una capa del visor, paginando con pausa de 1 s."""
    features, offset = [], 0
    while True:
        params = {"where": "1=1", "outFields": "*", "returnGeometry": str(geometria).lower(),
                  "orderByFields": "OBJECTID", "resultOffset": offset, "resultRecordCount": lote, "f": "json"}
        if geometria:
            params.update(outSR=4326, geometryPrecision=6, maxAllowableOffset=tolerancia)
        datos = _get_json(f"{ARCGIS}{capa}/query?{urllib.parse.urlencode(params)}")
        if "error" in datos:
            raise RuntimeError(f"{capa}: {datos['error']}")
        pagina = datos.get("features", [])
        features.extend(pagina)
        offset += len(pagina)
        print(f"  {capa}: {offset:,}", flush=True)
        if len(pagina) < lote and not datos.get("exceededTransferLimit"):
            return features
        time.sleep(1)


def _con_cache(cache_dir, nombre, descargar):
    ruta = os.path.join(cache_dir, nombre) if cache_dir else None
    if ruta and os.path.isfile(ruta):
        with open(ruta, encoding="utf-8") as handle:
            return json.load(handle)
    datos = descargar()
    if ruta:
        os.makedirs(cache_dir, exist_ok=True)
        with open(ruta, "w", encoding="utf-8") as handle:
            json.dump(datos, handle)
    return datos


def _municipios_oficiales():
    """Nombres DIVIPOLA del repo, para usar los mismos que dim_municipio."""
    if not os.path.isfile(MUNICIPIOS_XLSX):
        return None
    df = pd.read_excel(MUNICIPIOS_XLSX, "municipios", dtype={"cod_mpio": str})
    df["cod_mpio"] = df["cod_mpio"].str.zfill(5)
    return df[["cod_mpio", "departamento", "municipio"]]


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--cache", help="Directorio para guardar y reusar las descargas")
    parser.add_argument("--out", default=DEFAULT_OUT)
    args = parser.parse_args()

    registros = _con_cache(args.cache, "datos_gov_ixig-z8b5.json", lambda: _get_json(DATASET_URL))
    municipios_472 = _con_cache(args.cache, "capa_municipio.json", lambda: descargar_capa(CAPA_MUNICIPIO))
    cps = _con_cache(args.cache, "capa_codigo_postal.json", lambda: descargar_capa(CAPA_CODIGO_POSTAL, True, lote=500))
    barrios = _con_cache(args.cache, "capa_barrio.json", lambda: descargar_capa(CAPA_BARRIO, True))
    veredas = _con_cache(args.cache, "capa_vereda.json", lambda: descargar_capa(CAPA_VEREDA, True, 0.0001, 500))

    municipio_por_id = {str(m["attributes"]["Codigo"]): str(m["attributes"]["Codigo_DANE"]).zfill(5)
                        for m in municipios_472 if m["attributes"].get("Codigo_DANE")}
    municipio_por_cp = {reconstruir_codigo_postal(r["codigo_postal"]): codigo_municipio(r["codigo_municipio"])
                        for r in registros}
    indice = IndiceCodigoPostal(cps)
    capa = pd.concat([filas_capa(barrios, "barrio", municipio_por_id, indice),
                      filas_capa(veredas, "vereda", municipio_por_id, indice)], ignore_index=True)
    df = combinar(capa, filas_lista(registros), municipio_por_cp, localidades_bogota(registros),
                  _municipios_oficiales())
    df.to_csv(args.out, index=False)

    for nivel in ("barrio", "vereda"):
        sub = df[df["nivel"] == nivel]
        print(f"  {nivel}s: {len(sub):,} en {sub['cod_mpio'].nunique()} municipios | "
              f"con coordenadas {sub['lat'].notna().mean():.0%} | con codigo postal "
              f"{sub['codigo_postal'].notna().mean():.0%}")
    top = df[df["nivel"] == "barrio"].groupby("municipio").size().sort_values(ascending=False).head(10)
    print("  barrios por municipio:", ", ".join(f"{m} {n}" for m, n in top.items()))
    print(f"-> {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
