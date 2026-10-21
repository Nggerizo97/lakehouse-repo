# -*- coding: utf-8 -*-
"""Vigencia del aviso con y sin avistamientos del scraper (last_seen_at).

Ejecutar:  python -m pytest tests/test_listing_lifecycle.py -q
       o:  python tests/test_listing_lifecycle.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402

from src.quality.listing_lifecycle import add_lifecycle_columns  # noqa: E402


def _frame(with_sightings: bool) -> pd.DataFrame:
    df = pd.DataFrame(
        {
            "id_original": ["CC-1", "CC-2", "CC-3"],
            "fuente": ["ciencuadras_nuevo"] * 3,
            # CC-1 entró hace 90 días con el mismo precio; CC-3 es de ayer
            "fecha_extraccion": ["2026-07-20T10:00:00", "2026-07-20T10:00:00", "2026-10-20T10:00:00"],
        }
    )
    if with_sightings:
        # El scraper volvió a ver CC-1 ayer; CC-2 ya no aparece en el portal
        df["last_seen_at"] = ["2026-10-20T08:00:00", None, None]
    return df


def test_sin_avistamientos_aviso_estable_se_ve_retirado():
    out = add_lifecycle_columns(_frame(with_sightings=False)).set_index("id_original")
    assert not out.loc["CC-1", "is_active"]
    assert out.loc["CC-3", "is_active"]


def test_avistamiento_reciente_mantiene_vigente():
    out = add_lifecycle_columns(_frame(with_sightings=True)).set_index("id_original")
    assert out.loc["CC-1", "is_active"], "visto ayer: debe seguir vigente"
    assert out.loc["CC-1", "dias_sin_ver"] == 0
    assert not out.loc["CC-2", "is_active"], "no se volvió a ver: sigue fuera de la ventana"
    assert out.loc["CC-3", "is_active"]
    assert str(out.loc["CC-1", "last_seen_at"]).startswith("2026-10-20")


if __name__ == "__main__":
    test_sin_avistamientos_aviso_estable_se_ve_retirado()
    test_avistamiento_reciente_mantiene_vigente()
    print("OK — 2 pruebas")
