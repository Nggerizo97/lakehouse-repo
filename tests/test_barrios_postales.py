# -*- coding: utf-8 -*-
"""Catalogo de barrios y veredas de 4-72 (tools/build_barrios_postales.py).

Ejecutar:  python -m pytest tests/test_barrios_postales.py -q
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd  # noqa: E402
import pytest  # noqa: E402

from tools.build_barrios_postales import (  # noqa: E402
    FUENTE_CAPA, FUENTE_LISTA, LIMITE_CAMPO, IndiceCodigoPostal, clave_base, codigo_municipio,
    combinar, es_generico, filas_capa, filas_lista, localidades_bogota, reconstruir_codigo_postal,
    separar_lista,
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


def test_nombres_de_relleno():
    assert es_generico("Sin Definir")
    assert es_generico("CABECERA MUNICIPAL")
    assert not es_generico("Centro")


def _registro(cod_mpio, cp, barrios, nombre="MEDELLIN", veredas="Sin Informacion de Veredas"):
    return {"codigo_municipio": cod_mpio, "nombre_municipio": nombre, "codigo_postal": cp,
            "barrios_contenidos_en_el": barrios, "veredas_contenidas_en_el": veredas}


def test_lista_descarta_genericos_y_valida_departamento():
    df = filas_lista([_registro("5001", "50.014", "Calle Nueva- Sin Definir- Guayabal")])
    assert df["nombre"].tolist() == ["Calle Nueva", "Guayabal"]
    with pytest.raises(ValueError):
        filas_lista([_registro("5001", "110.111", "X Y Z")])


def test_localidad_de_bogota_por_codigo_postal():
    registros = [_registro("11001", "110.111", "Santa Barbara", nombre="LOCALIDAD USAQUEN"),
                 _registro("5001", "50.021", "El Poblado")]
    assert localidades_bogota(registros) == {"110111": "Usaquen"}


def _cuadrado(x, y, lado=1.0):
    return {"rings": [[[x, y], [x + lado, y], [x + lado, y + lado], [x, y + lado], [x, y]]]}


def test_capa_asigna_codigo_postal_y_coordenadas_por_poligono():
    cps = [{"attributes": {"Codigo_Postal": "050021"}, "geometry": _cuadrado(0, 0, 10)},
           {"attributes": {"Codigo_Postal": "050031"}, "geometry": _cuadrado(10, 0, 10)}]
    barrios = [
        {"attributes": {"Nombre": "Provenza", "MunicipioID": "10339", "ComunaID": "14", "Codigo": "1"},
         "geometry": _cuadrado(2, 2)},
        {"attributes": {"Nombre": "Laureles", "MunicipioID": "10339", "ComunaID": "0", "Codigo": "2"},
         "geometry": _cuadrado(12, 2)},
        {"attributes": {"Nombre": "Sin Definir", "MunicipioID": "10339"}, "geometry": _cuadrado(3, 3)},
    ]
    df = filas_capa(barrios, "barrio", {"10339": "05001"}, IndiceCodigoPostal(cps))
    assert df["nombre"].tolist() == ["Provenza", "Laureles"]
    assert df["codigo_postal"].tolist() == ["050021", "050031"]
    assert df["cod_mpio"].tolist() == ["05001", "05001"]
    assert df["comuna_id_4_72"].tolist() == ["14", None]  # "0" es "sin comuna"
    assert 2 < df["lon"].iloc[0] < 3 and 2 < df["lat"].iloc[0] < 3


def test_vereda_usa_codigo_dane_si_el_id_interno_falta():
    cps = [{"attributes": {"Codigo_Postal": "250001"}, "geometry": _cuadrado(0, 0, 10)}]
    veredas = [{"attributes": {"Nombre": "Fonquetá", "MunicipioID": None, "Codigo_DANE": "25175"},
                "geometry": _cuadrado(1, 1)}]
    df = filas_capa(veredas, "vereda", {}, IndiceCodigoPostal(cps))
    assert df.iloc[0]["cod_mpio"] == "25175"


def test_vereda_con_codigo_dane_de_relleno_y_codigo_postal_de_otro_departamento():
    cps = [{"attributes": {"Codigo_Postal": "445008"}, "geometry": _cuadrado(0, 0, 10)}]
    veredas = [{"attributes": {"Nombre": "El Morro", "MunicipioID": "99", "Codigo_DANE": "0000 "},
                "geometry": _cuadrado(1, 1)}]
    df = filas_capa(veredas, "vereda", {"99": "20013"}, IndiceCodigoPostal(cps))
    assert df.iloc[0]["cod_mpio"] == "20013"      # el relleno no se usa como municipio
    assert df.iloc[0]["codigo_postal"] is None    # 445008 es de La Guajira, no del Cesar


def test_combinar_prefiere_la_capa_y_suma_lo_que_solo_esta_en_la_lista():
    capa = pd.DataFrame([
        {"cod_mpio": "05001", "nivel": "barrio", "nombre": "Calle Nueva", "codigo_postal": "050014",
         "comuna_id_4_72": "15", "lat": 6.2, "lon": -75.6, "codigo_4_72": "9", "fuente": FUENTE_CAPA,
         "lista_truncada": False},
        {"cod_mpio": None, "nivel": "vereda", "nombre": "Yerbabuena", "codigo_postal": "250001",
         "comuna_id_4_72": None, "lat": 4.8, "lon": -74.0, "codigo_4_72": "7", "fuente": FUENTE_CAPA,
         "lista_truncada": False},
    ])
    lista = pd.DataFrame([
        {"cod_mpio": "05001", "nivel": "barrio", "nombre": "CALLE NUEVA", "codigo_postal": "050015",
         "fuente": FUENTE_LISTA, "lista_truncada": False},
        {"cod_mpio": "05001", "nivel": "barrio", "nombre": "Guayabal", "codigo_postal": "050023",
         "fuente": FUENTE_LISTA, "lista_truncada": True},
    ])
    df = combinar(capa, lista, {"250001": "25175"}, {})
    calle = df[df["key_barrio"] == "calle nueva"].iloc[0]
    assert len(df) == 3
    assert calle["nombre"] == "Calle Nueva" and calle["lat"] == 6.2
    assert calle["codigos_postales"] == "050014|050015"
    assert FUENTE_CAPA in calle["fuente"] and FUENTE_LISTA in calle["fuente"]
    assert df[df["key_barrio"] == "guayabal"].iloc[0]["lista_truncada"]
    # Vereda sin municipio en la capa: se toma el del codigo postal.
    assert df[df["key_barrio"] == "yerbabuena"].iloc[0]["cod_mpio"] == "25175"
