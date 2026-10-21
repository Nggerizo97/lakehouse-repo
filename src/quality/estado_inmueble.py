# -*- coding: utf-8 -*-
"""Estado del inmueble: nuevo, sobre planos, usado o remodelado.

Problema que resuelve
---------------------
Silver solo leia el estado de las columnas `estado_inmueble`/`status`/
`condition`, y casi ningun scraper las llena. Todo caia en "desconocido",
incluso ciencuadras_nuevo, que solo recorre /proyectos-vivienda-nueva. En la
app el filtro "Estado = nuevo" mostraba casi nada aunque Bronze tiene ~1.500
proyectos nuevos.

Como el indice de hashes (SCD2) no vuelve a emitir avisos ya vistos, no basta
con que el scraper agregue la columna desde ahora: el historico de Bronze se
reetiqueta aqui, en Silver.

Prioridad de senales
--------------------
1. Valor explicito del portal (estado_inmueble/status/condition).
2. Frases en titulo o URL ("sobre planos", "para estrenar", "/proyecto"...).
3. Seccion del portal de donde vino (ciencuadras_nuevo -> nuevo).
4. "desconocido".

En el titulo solo se buscan FRASES, no la palabra suelta "nuevo": hay barrios
como Barrio Nuevo, Pueblo Nuevo o Nuevo Country que la contienen.
"""

import re

from src.geo.normalize import normalize_text

ESTADO_NUEVO = "nuevo"
ESTADO_SOBRE_PLANOS = "sobre_planos"
ESTADO_USADO = "usado"
ESTADO_REMODELADO = "remodelado"
ESTADO_DESCONOCIDO = "desconocido"

# Portales cuya seccion ya define el estado de todos sus avisos.
SOURCE_DEFAULT_ESTADO = {
    "ciencuadras_nuevo": ESTADO_NUEVO,
    "ciencuadras_usado": ESTADO_USADO,
    "fincaraiz_proyectos": ESTADO_NUEVO,
    "metrocuadrado_proyectos": ESTADO_NUEVO,
}

# El orden importa: "proyecto nuevo sobre planos" es sobre_planos, no nuevo.
_RAW_RULES = [
    (ESTADO_SOBRE_PLANOS, ("plano", "preventa", "pre venta", "lanzamiento", "construccion", "proyecto")),
    (ESTADO_NUEVO, ("nuevo", "nueva", "new", "estrenar")),
    (ESTADO_USADO, ("usado", "usada", "second", "segunda")),
    (ESTADO_REMODELADO, ("remodelad", "renovad")),
]

_TEXT_RULES = [
    (ESTADO_SOBRE_PLANOS, re.compile(
        r"\b(sobre planos?|en planos?|preventa|pre venta|lanzamiento|en construccion)\b"
    )),
    (ESTADO_NUEVO, re.compile(
        r"\b(para estrenar|a estrenar|vivienda nueva|obra nueva|entrega inmediata"
        r"|proyecto|proyectos"
        r"|(apartamentos?|apartaestudios?|aptos?|casas?) nuev[oa]s?)\b"
    )),
    (ESTADO_USADO, re.compile(r"\b(usad[oa]s?|segunda mano|de segunda)\b")),
    (ESTADO_REMODELADO, re.compile(r"\b(remodelad[oa]s?|renovad[oa]s?)\b")),
]


def _from_raw(estado_raw):
    if estado_raw is None:
        return None
    text = normalize_text(estado_raw)
    if not text or text in {"n a", "na", "nan", "none", "null"}:
        return None
    for estado, keywords in _RAW_RULES:
        if any(k in text for k in keywords):
            return estado
    return None


def _from_text(*texts):
    joined = " ".join(normalize_text(t) for t in texts if t)
    if not joined:
        return None
    for estado, pattern in _TEXT_RULES:
        if pattern.search(joined):
            return estado
    return None


def infer_estado_inmueble(estado_raw=None, fuente=None, titulo=None, url=None) -> str:
    """Devuelve una de: nuevo, sobre_planos, usado, remodelado, desconocido."""
    return (
        _from_raw(estado_raw)
        or _from_text(titulo, url)
        or SOURCE_DEFAULT_ESTADO.get((fuente or "").strip().lower())
        or ESTADO_DESCONOCIDO
    )
