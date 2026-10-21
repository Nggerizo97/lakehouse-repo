# -*- coding: utf-8 -*-
"""Casos de regresion de la inferencia de estado del inmueble.

Ejecutar:  python -m pytest tests/test_estado_inmueble.py -q
       o:  python tests/test_estado_inmueble.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.quality.estado_inmueble import infer_estado_inmueble  # noqa: E402

# (estado_raw, fuente, titulo, url, esperado)
CASES = [
    # ciencuadras_nuevo no trae columna de estado: lo define la seccion
    (None, "ciencuadras_nuevo", "Apartamento en Bogotá", "https://www.ciencuadras.com/proyecto/torres-del-parque/123", "nuevo"),
    (None, "ciencuadras_nuevo", "Apartamento en Chía", None, "nuevo"),
    (None, "ciencuadras_usado", "Apartamento en Bogotá", "https://www.ciencuadras.com/inmueble/apartamento-en-venta/1", "usado"),
    # El titulo refina la seccion
    (None, "ciencuadras_nuevo", "Proyecto sobre planos en Cajicá", None, "sobre_planos"),
    # Frases en titulo/URL de portales mixtos
    (None, "fincaraiz", "Apartamento para estrenar en Envigado", None, "nuevo"),
    (None, "metrocuadrado", "Lanzamiento proyecto Altos del Bosque", None, "sobre_planos"),
    (None, "fincaraiz", "Apartamento en venta", "https://www.fincaraiz.com.co/proyecto-de-vivienda/reserva-del-rio/999", "nuevo"),
    (None, "properati", "Casa nueva en Jamundí", None, "nuevo"),
    (None, "mercadolibre", "Apto en preventa, entrega 2028", None, "sobre_planos"),
    (None, "metrocuadrado", "Apartamento remodelado en Chapinero", None, "remodelado"),
    # Barrios que contienen "nuevo" no deben marcar el aviso como nuevo
    (None, "fincaraiz", "Casa en Barrio Nuevo, Bogotá", None, "desconocido"),
    (None, "metrocuadrado", "Apartamento en Nuevo Country", None, "desconocido"),
    (None, "properati", "Casa en Pueblo Nuevo, Medellín", None, "desconocido"),
    # El valor explicito del portal manda sobre todo lo demas
    ("Usado", "ciencuadras_nuevo", "Apartamento para estrenar", None, "usado"),
    ("Nuevo", "bancolombia_tu360", "Casa en Rionegro", None, "nuevo"),
    ("Sobre planos", "bancolombia_tu360", None, None, "sobre_planos"),
    ("Proyecto nuevo sobre planos", None, None, None, "sobre_planos"),
    ("Para estrenar", None, None, None, "nuevo"),
    # Valores vacios del portal no bloquean las otras senales
    ("N/A", "ciencuadras_nuevo", None, None, "nuevo"),
    ("", "fincaraiz", "Apartamento para estrenar", None, "nuevo"),
    (None, None, None, None, "desconocido"),
]


def test_infer_estado_inmueble():
    failures = []
    for estado_raw, fuente, titulo, url, esperado in CASES:
        got = infer_estado_inmueble(estado_raw, fuente, titulo, url)
        if got != esperado:
            failures.append(f"{(estado_raw, fuente, titulo)} -> {got}, esperado {esperado}")
    assert not failures, "\n".join(failures)


if __name__ == "__main__":
    test_infer_estado_inmueble()
    print(f"OK — {len(CASES)} casos")
