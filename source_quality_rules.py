"""Reglas de calidad por fuente para la capa Silver.

Mantiene fuera del notebook los thresholds operativos de validacion
y las senales minimas esperadas por portal.
"""

SOURCE_PRICE_RULES = {
    "bancolombia_tu360": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "ciencuadras": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "ciencuadras_nuevo": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "ciencuadras_usado": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "facebook": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "fincaraiz": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "mercadolibre": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "metrocuadrado": {"min_price": 20_000_000, "max_price": 20_000_000_000},
    "properati": {"min_price": 20_000_000, "max_price": 20_000_000_000},
}

SOURCE_REQUIRED_SIGNALS = {
    "bancolombia_tu360": ["id_inmueble", "title", "price"],
    "ciencuadras": ["id_inmueble", "title", "price"],
    "ciencuadras_nuevo": ["id_inmueble", "title", "precio_num"],
    "ciencuadras_usado": ["id_inmueble", "title", "precio_num"],
    "facebook": ["id_inmueble", "title"],
    "fincaraiz": ["id_inmueble", "title", "precio_num"],
    "mercadolibre": ["id_inmueble", "title", "price"],
    "metrocuadrado": ["id_inmueble", "title", "precio_num"],
    "properati": ["id_inmueble", "title", "precio_num"],
}