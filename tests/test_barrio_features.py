# -*- coding: utf-8 -*-
"""Llave y features de barrio compartidas por 04, 05 y la app.

Regresion del bundle v8: 04_Model_Training no bajaba la columna barrio del
Gold, barrio_key quedaba en "barrio_otro" para todas las filas y el modelo
no podia distinguir La America de El Poblado.

Ejecutar:  python -m pytest tests/test_barrio_features.py -q
"""

import json
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

import pandas as pd  # noqa: E402

from src.geo.normalize import BARRIO_OTRO, ZONA_POSTAL_OTRA, barrio_key, zona_postal_key  # noqa: E402
from src.ml.scorer import add_barrio_features  # noqa: E402


def test_llave_normaliza_tildes_mayusculas_y_puntuacion():
    assert barrio_key("Medellín", "Santa Mónica") == "medellin | santa monica"
    assert barrio_key("medellin", "SANTA MONICA") == "medellin | santa monica"
    assert barrio_key("Medellín", "La Candelaria (Centro)") == "medellin | la candelaria centro"
    assert barrio_key("Bogotá, D.c.", "fonqueta") == "bogota d c | fonqueta"


def test_llave_sin_barrio_es_barrio_otro():
    # Gold escribe el barrio ausente como la cadena "None".
    for vacio in [None, "", "None", "nan", "<NA>", float("nan"), "   "]:
        assert barrio_key("Medellín", vacio) == BARRIO_OTRO
    assert barrio_key("", "La América") == BARRIO_OTRO


def _frame(**cols):
    base = {"city_token": ["Medellín"], "area_m2": [100.0], "precio_m2_mediano_ciudad": [5.0e6]}
    base.update(cols)
    return pd.DataFrame(base)


BUNDLE = {
    "barrio_stats": [
        {"barrio_key": "medellin | la america", "precio_m2_mediano_barrio": 6.0e6, "n_barrio": 40},
        {"barrio_key": "medellin | fonqueta", "precio_m2_mediano_barrio": 3.0e6, "n_barrio": 12},
    ],
    "zona_postal_stats": [
        {"zona_postal_key": "050021", "precio_m2_mediano_zona_postal": 9.0e6, "n_zona_postal": 300},
    ],
}


def test_barrio_conocido_usa_la_mediana_del_bundle():
    r = add_barrio_features(_frame(barrio=["La América"]), BUNDLE).iloc[0]
    assert r["barrio_en_modelo"]
    assert r["barrio_key"] == "medellin | la america"
    assert r["precio_m2_mediano_barrio"] == 6.0e6
    assert r["precio_estimado_barrio_area"] == 6.0e8
    assert r["barrio_vs_ciudad_ratio"] == 1.2


def test_texto_libre_cubre_el_barrio_oficial_vacio():
    r = add_barrio_features(_frame(barrio=["None"], barrio_texto=["Fonqueta"]), BUNDLE).iloc[0]
    assert r["barrio_key"] == "medellin | fonqueta"


def test_barrio_desconocido_cae_a_la_ciudad():
    r = add_barrio_features(_frame(barrio=["Barrio Inventado"]), BUNDLE).iloc[0]
    assert not r["barrio_en_modelo"]
    assert r["barrio_key"] == BARRIO_OTRO
    assert r["precio_m2_mediano_barrio"] == 5.0e6
    assert r["barrio_vs_ciudad_ratio"] == 1.0


def test_entrenamiento_baja_la_columna_barrio_del_gold():
    """La linea del bug: barrio y barrio_texto deben entrar al select de Spark."""
    with open(os.path.join(ROOT, "notebooks", "04_Model_Training.ipynb"), encoding="utf-8") as fh:
        celdas = ["".join(c["source"]) for c in json.load(fh)["cells"] if c["cell_type"] == "code"]
    codigo = next(c for c in celdas if "all_cols = list(dict.fromkeys(" in c)
    select = codigo.split("all_cols = list(dict.fromkeys(", 1)[1].split("))", 1)[0]
    assert "barrio_cols" in select
    definicion = codigo.split("barrio_cols = [c for c in [", 1)[1].split("]", 1)[0]
    for columna in ('"barrio"', '"barrio_texto"', '"codigo_postal"'):
        assert columna in definicion, columna
    assert "from src.geo.normalize import barrio_key" in codigo
    assert "zona_postal_key as _zona_postal_key" in codigo


def test_zona_postal_key():
    assert zona_postal_key("050021") == "050021"
    assert zona_postal_key(50021) == "050021"        # leido como numero
    assert zona_postal_key("50021.0") == "050021"
    for vacio in [None, "", "nan", "5001", "zona"]:
        assert zona_postal_key(vacio) == ZONA_POSTAL_OTRA


def test_barrio_desconocido_usa_su_zona_postal_antes_que_la_ciudad():
    con_zona = add_barrio_features(_frame(barrio=["Castropol"], codigo_postal=["050021"]), BUNDLE).iloc[0]
    assert not con_zona["barrio_en_modelo"]
    assert con_zona["precio_m2_mediano_zona_postal"] == 9.0e6
    assert con_zona["precio_m2_mediano_barrio"] == 9.0e6        # barrio -> zona postal
    sin_zona = add_barrio_features(_frame(barrio=["Castropol"], codigo_postal=["999999"]), BUNDLE).iloc[0]
    assert sin_zona["precio_m2_mediano_barrio"] == 5.0e6        # zona desconocida -> ciudad


def test_barrio_conocido_no_cambia_por_la_zona():
    r = add_barrio_features(_frame(barrio=["La América"], codigo_postal=["050021"]), BUNDLE).iloc[0]
    assert r["precio_m2_mediano_barrio"] == 6.0e6
    assert r["precio_m2_mediano_zona_postal"] == 9.0e6


def test_bundle_sin_zonas_se_comporta_como_antes():
    viejo = {"barrio_stats": BUNDLE["barrio_stats"]}
    r = add_barrio_features(_frame(barrio=["Castropol"], codigo_postal=["050021"]), viejo).iloc[0]
    assert r["precio_m2_mediano_barrio"] == 5.0e6
