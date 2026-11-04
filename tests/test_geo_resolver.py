# -*- coding: utf-8 -*-
"""Casos de regresion del resolver geografico.

Cada caso viene de un patron real observado en gold/app_inmuebles_scored.
Ejecutar:  python -m pytest tests/test_geo_resolver.py -q
       o:  python tests/test_geo_resolver.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.geo.geo_resolver import get_resolver  # noqa: E402
from src.geo.normalize import normalize_location  # noqa: E402

# (ubicacion, titulo, municipio_esperado, departamento_esperado)
MUNICIPIO_CASES = [
    ("Bogotá, Usaquén, San Antonio Nor - Occidental", None, "Bogotá, D.c.", "Bogotá, D.c."),
    ("Bogotá, Puente Aranda, La Trinidad", None, "Bogotá, D.c.", "Bogotá, D.c."),
    ("Valle De Lili, Cali", None, "Santiago de Cali", "Valle del Cauca"),
    ("Rodadero, Santa Marta", None, "Santa Marta", "Magdalena"),
    ("Santa Rita, Chía", None, "Chía", "Cundinamarca"),
    ("Itagüí, Comuna 2, Monte Verde", None, "Itagüí", "Antioquia"),
    ("El Poblado, Medellín", None, "Medellín", "Antioquia"),
    ("Cartagena de Indias, Bocagrande", None, "Cartagena de Indias", "Bolívar"),
    ("San José de Cúcuta, Caobos", None, "San José de Cúcuta", "Norte de Santander"),
    ("Envigado, Antioquia", None, "Envigado", "Antioquia"),
    # Homonimos: el departamento mencionado debe mandar
    ("Rionegro, Santander", None, "Rionegro", "Santander"),
    ("Rionegro, Antioquia", None, "Rionegro", "Antioquia"),
    ("Barbosa, Santander", None, "Barbosa", "Santander"),
    ("La Unión, Valle del Cauca", None, "La Unión", "Valle del Cauca"),
    # Prioridad: Armenia capital de Quindio gana sobre Armenia (Antioquia)
    ("Armenia", None, "Armenia", "Quindío"),
    # El titulo no debe secuestrar el municipio de la ubicacion
    ("Chía, Cundinamarca", "Apartamento en venta cerca a Bogotá", "Chía", "Cundinamarca"),
]

# (ubicacion, titulo) -> barrio_texto NO debe ser una de estas cadenas
GARBAGE_BARRIOS = {
    "en", "d c", "d c en", "en en", "comuna en", "no en", "de indias en",
    "en o", "urbana d c", "sector", "barrio", "zona", "colombia",
}

BARRIO_CASES = [
    ("Bogotá, Chapinero, El Nogal", None, "nogal"),
    ("Medellín, El Poblado, Provenza", None, "provenza"),
    ("Cali, Ciudad Jardín", None, "ciudad jardin"),
]


def _resolve(ubicacion, titulo=None):
    return get_resolver().resolve(ubicacion, titulo)


def test_normalize_strips_dc_noise():
    assert "d c" not in normalize_location("Bogotá, D.C.")
    assert normalize_location("Bogotá, D.C.") == "bogota"


def test_word_boundary_matching():
    """'en' no debe hacer match dentro de 'medellin'.

    Este era el bug que llenaba sector_mercado con 'en' en 10.972 filas.
    """
    from src.geo.normalize import contains_phrase

    assert not contains_phrase("medellin el poblado", "en")
    assert contains_phrase("apartamento en medellin", "en")
    assert not contains_phrase("calima el darien", "cali")


def test_municipio_resolution():
    failures = []
    for ubicacion, titulo, expected_mpio, expected_dpto in MUNICIPIO_CASES:
        got = _resolve(ubicacion, titulo)
        if got["municipio"] != expected_mpio or got["departamento"] != expected_dpto:
            failures.append(
                f"{ubicacion!r} -> {got['municipio']} / {got['departamento']} "
                f"(esperado {expected_mpio} / {expected_dpto})"
            )
    assert not failures, "\n".join(failures)


def test_barrio_texto_never_garbage():
    failures = []
    for ubicacion, titulo, _, _ in MUNICIPIO_CASES:
        got = _resolve(ubicacion, titulo)
        candidate = got.get("barrio_texto")
        if candidate and candidate in GARBAGE_BARRIOS:
            failures.append(f"{ubicacion!r} -> barrio_texto={candidate!r}")
    assert not failures, "\n".join(failures)


def test_barrio_detection():
    from src.geo.normalize import normalize_text

    failures = []
    for ubicacion, titulo, expected_fragment in BARRIO_CASES:
        got = _resolve(ubicacion, titulo)
        found = " ".join(
            normalize_text(part)
            for part in (got.get("barrio"), got.get("barrio_texto"))
            if part
        )
        if expected_fragment not in found:
            failures.append(f"{ubicacion!r} -> barrio={found!r} (esperaba {expected_fragment!r})")
    assert not failures, "\n".join(failures)


def test_coordinates_present_when_municipio_resolved():
    got = _resolve("El Poblado, Medellín")
    assert got["lat"] is not None and got["lon"] is not None
    assert 3.0 < got["lat"] < 9.0, got["lat"]
    assert -78.0 < got["lon"] < -73.0, got["lon"]


def test_dane_codes_are_wellformed():
    got = _resolve("Bogotá, Chapinero")
    assert got["cod_dpto"] == "11"
    assert got["cod_mpio"] == "11001"


# (ubicacion, barrio esperado, barrio_zona esperada o None). Usa el catalogo
# 4-72 de reference/barrios_postales_4_72.csv que build_all suma a dim_barrio.
BARRIO_OFICIAL_CASES = [
    # Barrios que se llaman como un municipio de otro departamento: antes se
    # descartaban como "contexto" y el aviso quedaba sin barrio.
    ("Barranquilla, Comuna 19, Andalucia", "andalucia", None),
    ("La Cumbre, Barranquilla", "la cumbre", None),
    ("Barranquilla, Comuna 19, Betania", "betania", None),
    # El barrio gana sobre la comuna o localidad, que pasa a barrio_zona.
    ("Medellín, El Poblado, Provenza", "provenza", "el poblado"),
    ("Medellín, Laureles, Conquistadores", "conquistadores", "laureles"),
    ("Bogotá, Usaquén, Cedritos", "cedritos", "usaquen"),
    ("Bogotá, Chapinero, Rosales", "rosales", "chapinero"),
    # Sola, la comuna sigue siendo el barrio.
    ("Medellín, El Poblado", "el poblado", None),
    # Mismo nombre con otro corte de palabras.
    ("Cartagena de Indias, Bocagrande", "boca grande", None),
    ("Castillo Grande, Cartagena", "castillogrande", None),
    # Veredas de municipios sin barrios oficiales.
    ("Chía, Fonquetá", "fonqueta", None),
    ("Vereda Yerbabuena, Chía", "yerbabuena", None),
]


def test_barrio_oficial_y_zona():
    from src.geo.normalize import normalize_text

    failures = []
    for ubicacion, barrio, zona in BARRIO_OFICIAL_CASES:
        got = _resolve(ubicacion)
        if normalize_text(got.get("barrio")) != barrio:
            failures.append(f"{ubicacion!r} -> barrio={got.get('barrio')!r} (esperado {barrio!r})")
        elif zona and normalize_text(got.get("barrio_zona")) != zona:
            failures.append(f"{ubicacion!r} -> barrio_zona={got.get('barrio_zona')!r} (esperado {zona!r})")
    assert not failures, "\n".join(failures)


def test_codigo_postal_del_barrio_oficial():
    got = _resolve("Bogotá, Usaquén, Cedritos")
    assert got["codigo_postal"] and got["codigo_postal"].startswith("11")
    assert _resolve("Bogotá")["codigo_postal"] is None


def test_ciudad_principal_en_el_titulo_no_es_barrio():
    got = _resolve("Chía, Cundinamarca", "Apartamento en venta cerca a Bogotá")
    assert got["municipio"] == "Chía"
    assert "bogota" not in (got.get("barrio_texto") or "")


def test_empty_input_is_safe():
    for value in (None, "", "   ", "nan"):
        got = _resolve(value)
        assert got["geo_match_level"] in {"sin_match", "municipio", "barrio", "departamento"}


if __name__ == "__main__":
    import traceback

    tests = [value for name, value in sorted(globals().items()) if name.startswith("test_")]
    failed = 0
    for test in tests:
        try:
            test()
            print(f"  PASS  {test.__name__}")
        except AssertionError as exc:
            failed += 1
            print(f"  FAIL  {test.__name__}\n{exc}")
        except Exception:
            failed += 1
            print(f"  ERROR {test.__name__}\n{traceback.format_exc()}")
    print(f"\n{len(tests) - failed}/{len(tests)} pruebas OK")
    sys.exit(1 if failed else 0)
