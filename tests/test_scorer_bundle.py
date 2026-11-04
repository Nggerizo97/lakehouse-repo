# -*- coding: utf-8 -*-
"""src/ml/scorer.py acepta el bundle en los dos formatos que circulan.

Regresion: 05_Batch_Inference entrega el bundle ya cargado ("model" es el
xgb.Booster y "preprocessor" el ColumnTransformer), pero el scorer solo leia
"model_json"/"preprocessor_pickle", caia al respaldo "pickle" y fallaba con
"Expecting data to be a DMatrix object, got DataFrame".

Ejecutar:  python -m pytest tests/test_scorer_bundle.py -q
"""

import os
import pickle
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import pytest  # noqa: E402

xgb = pytest.importorskip("xgboost")
pytest.importorskip("sklearn")
from sklearn.compose import ColumnTransformer  # noqa: E402
from sklearn.preprocessing import OneHotEncoder  # noqa: E402

from src.ml.scorer import score_dataframe  # noqa: E402

FEATURES = ["area_m2", "habitaciones", "city_token"]


def _avisos(n=200, seed=1):
    rng = np.random.default_rng(seed)
    ciudades = rng.choice(["Medellín", "Bogotá, D.c."], n)
    area = rng.uniform(40, 250, n)
    return pd.DataFrame({
        "area_m2": area,
        "habitaciones": rng.integers(1, 5, n).astype(float),
        "city_token": ciudades,
        "precio_num": area * np.where(ciudades == "Medellín", 6e6, 8e6),
    })


@pytest.fixture(scope="module")
def modelo():
    avisos = _avisos()
    pre = ColumnTransformer([
        ("num", "passthrough", ["area_m2", "habitaciones"]),
        ("cat", OneHotEncoder(handle_unknown="ignore"), ["city_token"]),
    ])
    matriz = pre.fit_transform(avisos[FEATURES])
    bst = xgb.train({"objective": "reg:squarederror", "verbosity": 0},
                    xgb.DMatrix(matriz, label=np.log1p(avisos["precio_num"])), num_boost_round=30)
    return bst, pre


def _base():
    return {"strategy": "absolute", "feature_cols": FEATURES, "market_meta": {}, "metrics": {"mape": 20.0}}


def _puntuar(bundle):
    return score_dataframe(_avisos(n=6, seed=9), bundle)


def test_bundle_como_lo_arma_05(modelo):
    bst, pre = modelo
    out = _puntuar({**_base(), "model": bst, "booster": bst, "preprocessor": pre})
    assert not out["estado_inversion"].astype(str).str.startswith("Error").any(), out["estado_inversion"].tolist()
    assert np.isfinite(out["precio_predicho"]).all()


def test_bundle_json_crudo_da_lo_mismo(modelo):
    bst, pre = modelo
    cargado = _puntuar({**_base(), "model": bst, "booster": bst, "preprocessor": pre})
    crudo = _puntuar({**_base(), "model_json": bst.save_raw("json").decode("utf-8"),
                      "preprocessor_pickle": pickle.dumps(pre).decode("latin1")})
    assert np.allclose(cargado["precio_predicho"], crudo["precio_predicho"])


def test_sin_preprocesador_el_error_dice_la_causa(modelo):
    bst, _ = modelo
    out = _puntuar({**_base(), "model": bst})
    assert out["precio_predicho"].isna().all()
    assert out["estado_inversion"].iloc[0].startswith("Error Crítico: El bundle trae el Booster")
