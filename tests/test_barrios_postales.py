# -*- coding: utf-8 -*-
"""Catalogo barrio -> codigo postal de 4-72 (tools/build_barrios_postales.py).

Ejecutar:  python -m pytest tests/test_barrios_postales.py -q
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest  # noqa: E402

from tools.build_barrios_postales import (  # noqa: E402
    LIMITE_CAMPO, clave_base, codigo_municipio, construir, reconstruir_codigo_postal, separar_lista,
)


@pytest.mark.parametrize("crudo, esperado", [
    ("153.42", "153420"),   # 153420 sin el cero final
    ("55.03", "055030"),    # Antioquia: pierde el cero inicial y el final
    ("880.007", "880007"),
    ("50.021", "050021"),   # El Poblado
])
def test_reconstruye_codigo_postal(crudo, esperado):
    assert reconstruir_codigo_postal(crudo) == esperado


def test_codigo_municipio_recupera_cero_inicial():
    assert codigo_municipio("5001") == "05001"
    assert codigo_municipio("11001") == "11001"


def test_separa_sin_romper_guiones_internos():
    nombres, truncada = separar_lista("Altos del Poblado- El Nogal-Los Almendros- Asomadera No.2")
    assert nombres == ["Altos del Poblado", "El Nogal-Los Almendros", "Asomadera No.2"]
    assert not truncada
    assert separar_lista("Sin Informacion de Barrios") == ([], False)
    assert separar_lista("442- Villa Niza") == (["Villa Niza"], False)


def test_lista_cortada_descarta_el_ultimo_fragmento():
    texto = ("Barrio Largo- " * 30)[: LIMITE_CAMPO - 8] + "Urbaniza"
    nombres, truncada = separar_lista(texto)
    assert truncada
    assert "Urbaniza" not in nombres


@pytest.mark.parametrize("oficial, base", [
    ("Los Balsos No.2", "balsos"),
    ("El Chicó", "chico"),
    ("Chicó Norte II Sector", "chico norte"),
    ("Belén (Antes Provivienda I Etapa)", "belen"),
    ("BARRIO EL PARQUE", "parque"),
    ("Chapinero Central", "chapinero central"),
])
def test_clave_base_tolera_variantes_sin_fusionar_barrios(oficial, base):
    assert clave_base(oficial) == base


def test_construir_agrupa_un_barrio_en_varios_codigos():
    registros = [
        {"codigo_municipio": "5001", "nombre_municipio": "MEDELLIN", "codigo_postal": "50.014", "tipo": "Urbano",
         "barrios_contenidos_en_el": "Calle Nueva- Guayabal", "veredas_contenidas_en_el": "Sin Informacion de Veredas"},
        {"codigo_municipio": "5001", "nombre_municipio": "MEDELLIN", "codigo_postal": "50.015", "tipo": "Urbano",
         "barrios_contenidos_en_el": "Calle Nueva", "veredas_contenidas_en_el": "Sin Informacion de Veredas"},
    ]
    df = construir(registros)
    calle = df[df["key_barrio"] == "calle nueva"].iloc[0]
    assert len(df) == 2
    assert calle["codigo_postal"] == "050014"
    assert calle["codigos_postales"] == "050014|050015"
    assert calle["cod_mpio"] == "05001"


def test_codigo_postal_de_otro_departamento_es_error():
    registro = {"codigo_municipio": "5001", "codigo_postal": "110.111",
                "barrios_contenidos_en_el": "X Y Z", "veredas_contenidas_en_el": None}
    with pytest.raises(ValueError):
        construir([registro])
