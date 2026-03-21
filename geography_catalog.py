"""Catalogo geografico canonico para clasificacion de ubicaciones.

Centraliza aliases de ciudades y metadatos utiles para categorizacion:
- ciudad canonica
- departamento
- region macro
"""

CITY_CATALOG = {
    "bogota": {
        "aliases": ["bogota", "bogota d c", "bogota dc", "bogota distrito capital"],
        "department": "bogota_dc",
        "region": "andina",
    },
    "medellin": {"aliases": ["medellin"], "department": "antioquia", "region": "andina"},
    "cali": {"aliases": ["cali", "santiago de cali"], "department": "valle_del_cauca", "region": "pacifica"},
    "barranquilla": {"aliases": ["barranquilla", "bquilla"], "department": "atlantico", "region": "caribe"},
    "cartagena": {"aliases": ["cartagena", "cartagena de indias"], "department": "bolivar", "region": "caribe"},
    "bucaramanga": {"aliases": ["bucaramanga"], "department": "santander", "region": "andina"},
    "pereira": {"aliases": ["pereira"], "department": "risaralda", "region": "andina"},
    "manizales": {"aliases": ["manizales"], "department": "caldas", "region": "andina"},
    "cucuta": {"aliases": ["cucuta", "san jose de cucuta"], "department": "norte_de_santander", "region": "andina"},
    "ibague": {"aliases": ["ibague"], "department": "tolima", "region": "andina"},
    "santa marta": {"aliases": ["santa marta", "sta marta"], "department": "magdalena", "region": "caribe"},
    "villavicencio": {"aliases": ["villavicencio"], "department": "meta", "region": "orinoquia"},
    "pasto": {"aliases": ["pasto", "san juan de pasto"], "department": "narino", "region": "pacifica"},
    "monteria": {"aliases": ["monteria"], "department": "cordoba", "region": "caribe"},
    "neiva": {"aliases": ["neiva"], "department": "huila", "region": "andina"},
    "armenia": {"aliases": ["armenia"], "department": "quindio", "region": "andina"},
    "popayan": {"aliases": ["popayan"], "department": "cauca", "region": "pacifica"},
    "valledupar": {"aliases": ["valledupar"], "department": "cesar", "region": "caribe"},
    "sincelejo": {"aliases": ["sincelejo"], "department": "sucre", "region": "caribe"},
    "tunja": {"aliases": ["tunja"], "department": "boyaca", "region": "andina"},
    "envigado": {"aliases": ["envigado"], "department": "antioquia", "region": "andina"},
    "sabaneta": {"aliases": ["sabaneta"], "department": "antioquia", "region": "andina"},
    "itagui": {"aliases": ["itagui"], "department": "antioquia", "region": "andina"},
    "bello": {"aliases": ["bello"], "department": "antioquia", "region": "andina"},
    "soacha": {"aliases": ["soacha"], "department": "cundinamarca", "region": "andina"},
    "chia": {"aliases": ["chia", "chia cundinamarca"], "department": "cundinamarca", "region": "andina"},
    "zipaquira": {"aliases": ["zipaquira"], "department": "cundinamarca", "region": "andina"},
    "cajica": {"aliases": ["cajica"], "department": "cundinamarca", "region": "andina"},
    "cota": {"aliases": ["cota"], "department": "cundinamarca", "region": "andina"},
    "funza": {"aliases": ["funza"], "department": "cundinamarca", "region": "andina"},
    "mosquera": {"aliases": ["mosquera"], "department": "cundinamarca", "region": "andina"},
    "tocancipa": {"aliases": ["tocancipa"], "department": "cundinamarca", "region": "andina"},
    "la calera": {"aliases": ["la calera"], "department": "cundinamarca", "region": "andina"},
    "sopo": {"aliases": ["sopo"], "department": "cundinamarca", "region": "andina"},
    "rionegro": {"aliases": ["rionegro"], "department": "antioquia", "region": "andina"},
    "girardot": {"aliases": ["girardot"], "department": "cundinamarca", "region": "andina"},
    "floridablanca": {"aliases": ["floridablanca"], "department": "santander", "region": "andina"},
    "piedecuesta": {"aliases": ["piedecuesta"], "department": "santander", "region": "andina"},
}

MARKET_CATALOG = {
    "bogota_metropolitana": [
        "bogota",
        "soacha",
        "chia",
        "zipaquira",
        "cajica",
        "cota",
        "funza",
        "mosquera",
        "tocancipa",
        "la calera",
        "sopo",
        "girardot",
    ],
    "valle_aburra": [
        "medellin",
        "envigado",
        "sabaneta",
        "itagui",
        "bello",
    ],
    "oriente_antioqueno": [
        "rionegro",
    ],
    "cali_metropolitana": [
        "cali",
    ],
    "barranquilla_metropolitana": [
        "barranquilla",
    ],
    "cartagena_metropolitana": [
        "cartagena",
    ],
    "bucaramanga_metropolitana": [
        "bucaramanga",
        "floridablanca",
        "piedecuesta",
    ],
    "eje_cafetero": [
        "pereira",
        "manizales",
        "armenia",
    ],
    "cucuta_metropolitana": [
        "cucuta",
    ],
    "santa_marta_metropolitana": [
        "santa marta",
    ],
    "villavicencio_metropolitana": [
        "villavicencio",
    ],
    "pasto_metropolitana": [
        "pasto",
    ],
    "monteria_metropolitana": [
        "monteria",
    ],
    "neiva_metropolitana": [
        "neiva",
    ],
    "popayan_metropolitana": [
        "popayan",
    ],
    "valledupar_metropolitana": [
        "valledupar",
    ],
    "sincelejo_metropolitana": [
        "sincelejo",
    ],
    "tunja_metropolitana": [
        "tunja",
    ],
    "ibague_metropolitana": [
        "ibague",
    ],
}

CITY_ALIAS_TO_CANONICAL = {}
CITY_TO_DEPARTMENT = {}
CITY_TO_REGION = {}
CITY_TO_MARKET = {}

for canonical_city, payload in CITY_CATALOG.items():
    CITY_ALIAS_TO_CANONICAL[canonical_city] = canonical_city
    CITY_TO_DEPARTMENT[canonical_city] = payload["department"]
    CITY_TO_REGION[canonical_city] = payload["region"]
    for alias in payload.get("aliases", []):
        CITY_ALIAS_TO_CANONICAL[alias] = canonical_city

for market_token, cities in MARKET_CATALOG.items():
    for canonical_city in cities:
        CITY_TO_MARKET[canonical_city] = market_token

SORTED_CITY_ALIASES = sorted(CITY_ALIAS_TO_CANONICAL.keys(), key=len, reverse=True)
