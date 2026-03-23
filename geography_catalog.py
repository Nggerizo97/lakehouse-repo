"""Catalogo geografico canonico para clasificacion de ubicaciones.

Centraliza aliases de ciudades y metadatos utiles para categorizacion:
- ciudad canonica
- departamento
- region macro
"""

CITY_CATALOG = {
    # ── Capitales departamentales / ciudades principales ──────
    "bogota": {
        "aliases": ["bogota", "bogota d c", "bogota dc", "bogota d.c", "bogota d.c.", "bogota distrito capital"],
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

    # ── Antioquia — Valle de Aburrá ──────────────────────────
    "envigado": {"aliases": ["envigado"], "department": "antioquia", "region": "andina"},
    "sabaneta": {"aliases": ["sabaneta"], "department": "antioquia", "region": "andina"},
    "itagui": {"aliases": ["itagui"], "department": "antioquia", "region": "andina"},
    "bello": {"aliases": ["bello"], "department": "antioquia", "region": "andina"},
    "la estrella": {"aliases": ["la estrella"], "department": "antioquia", "region": "andina"},
    "copacabana": {"aliases": ["copacabana"], "department": "antioquia", "region": "andina"},
    "caldas_antioquia": {"aliases": ["caldas"], "department": "antioquia", "region": "andina"},
    "barbosa_antioquia": {"aliases": ["barbosa"], "department": "antioquia", "region": "andina"},

    # ── Antioquia — Oriente Antioqueño ───────────────────────
    "rionegro": {"aliases": ["rionegro"], "department": "antioquia", "region": "andina"},
    "la ceja": {"aliases": ["la ceja"], "department": "antioquia", "region": "andina"},
    "el retiro": {"aliases": ["el retiro"], "department": "antioquia", "region": "andina"},
    "guarne": {"aliases": ["guarne"], "department": "antioquia", "region": "andina"},
    "marinilla": {"aliases": ["marinilla"], "department": "antioquia", "region": "andina"},
    "el carmen de viboral": {"aliases": ["el carmen de viboral", "carmen de viboral"], "department": "antioquia", "region": "andina"},
    "penol": {"aliases": ["penol"], "department": "antioquia", "region": "andina"},
    "guatape": {"aliases": ["guatape"], "department": "antioquia", "region": "andina"},
    "san vicente": {"aliases": ["san vicente"], "department": "antioquia", "region": "andina"},

    # ── Antioquia — Occidente Antioqueño ─────────────────────
    "sopetran": {"aliases": ["sopetran"], "department": "antioquia", "region": "andina"},
    "santa fe de antioquia": {"aliases": ["santa fe de antioquia", "santafe de antioquia"], "department": "antioquia", "region": "andina"},
    "san jeronimo": {"aliases": ["san jeronimo"], "department": "antioquia", "region": "andina"},

    # ── Antioquia — Suroeste / Norte ─────────────────────────
    "amaga": {"aliases": ["amaga"], "department": "antioquia", "region": "andina"},
    "jardin": {"aliases": ["jardin"], "department": "antioquia", "region": "andina"},
    "fredonia": {"aliases": ["fredonia"], "department": "antioquia", "region": "andina"},
    "san pedro de los milagros": {"aliases": ["san pedro de los milagros"], "department": "antioquia", "region": "andina"},

    # ── Cundinamarca — Sabana / Bogotá metropolitana ─────────
    "soacha": {"aliases": ["soacha"], "department": "cundinamarca", "region": "andina"},
    "chia": {"aliases": ["chia", "chia cundinamarca"], "department": "cundinamarca", "region": "andina"},
    "zipaquira": {"aliases": ["zipaquira"], "department": "cundinamarca", "region": "andina"},
    "cajica": {"aliases": ["cajica"], "department": "cundinamarca", "region": "andina"},
    "cota": {"aliases": ["cota"], "department": "cundinamarca", "region": "andina"},
    "funza": {"aliases": ["funza"], "department": "cundinamarca", "region": "andina"},
    "mosquera": {"aliases": ["mosquera"], "department": "cundinamarca", "region": "andina"},
    "madrid": {"aliases": ["madrid"], "department": "cundinamarca", "region": "andina"},
    "tocancipa": {"aliases": ["tocancipa"], "department": "cundinamarca", "region": "andina"},
    "la calera": {"aliases": ["la calera"], "department": "cundinamarca", "region": "andina"},
    "sopo": {"aliases": ["sopo"], "department": "cundinamarca", "region": "andina"},
    "tenjo": {"aliases": ["tenjo"], "department": "cundinamarca", "region": "andina"},
    "tabio": {"aliases": ["tabio"], "department": "cundinamarca", "region": "andina"},
    "guasca": {"aliases": ["guasca"], "department": "cundinamarca", "region": "andina"},
    "subachoque": {"aliases": ["subachoque"], "department": "cundinamarca", "region": "andina"},
    "sesquile": {"aliases": ["sesquile"], "department": "cundinamarca", "region": "andina"},
    "choconta": {"aliases": ["choconta"], "department": "cundinamarca", "region": "andina"},
    "choachi": {"aliases": ["choachi"], "department": "cundinamarca", "region": "andina"},

    # ── Cundinamarca — Turismo / Tierra caliente ─────────────
    "girardot": {"aliases": ["girardot"], "department": "cundinamarca", "region": "andina"},
    "fusagasuga": {"aliases": ["fusagasuga"], "department": "cundinamarca", "region": "andina"},
    "anapoima": {"aliases": ["anapoima"], "department": "cundinamarca", "region": "andina"},
    "la mesa": {"aliases": ["la mesa"], "department": "cundinamarca", "region": "andina"},
    "villeta": {"aliases": ["villeta"], "department": "cundinamarca", "region": "andina"},
    "silvania": {"aliases": ["silvania"], "department": "cundinamarca", "region": "andina"},
    "nilo": {"aliases": ["nilo"], "department": "cundinamarca", "region": "andina"},
    "apulo": {"aliases": ["apulo"], "department": "cundinamarca", "region": "andina"},
    "ricaurte": {"aliases": ["ricaurte"], "department": "cundinamarca", "region": "andina"},
    "pacho": {"aliases": ["pacho"], "department": "cundinamarca", "region": "andina"},

    # ── Valle del Cauca — Cali metropolitana ─────────────────
    "jamundi": {"aliases": ["jamundi"], "department": "valle_del_cauca", "region": "pacifica"},
    "yumbo": {"aliases": ["yumbo"], "department": "valle_del_cauca", "region": "pacifica"},
    "palmira": {"aliases": ["palmira"], "department": "valle_del_cauca", "region": "pacifica"},
    "candelaria": {"aliases": ["candelaria"], "department": "valle_del_cauca", "region": "pacifica"},
    "dagua": {"aliases": ["dagua"], "department": "valle_del_cauca", "region": "pacifica"},
    "la cumbre": {"aliases": ["la cumbre"], "department": "valle_del_cauca", "region": "pacifica"},

    # ── Valle del Cauca — Norte del Valle ────────────────────
    "cartago": {"aliases": ["cartago"], "department": "valle_del_cauca", "region": "pacifica"},
    "tulua": {"aliases": ["tulua"], "department": "valle_del_cauca", "region": "pacifica"},
    "alcala": {"aliases": ["alcala"], "department": "valle_del_cauca", "region": "pacifica"},

    # ── Atlántico — Barranquilla metropolitana ───────────────
    "soledad": {"aliases": ["soledad"], "department": "atlantico", "region": "caribe"},
    "puerto colombia": {"aliases": ["puerto colombia"], "department": "atlantico", "region": "caribe"},
    "juan de acosta": {"aliases": ["juan de acosta"], "department": "atlantico", "region": "caribe"},
    "tubara": {"aliases": ["tubara"], "department": "atlantico", "region": "caribe"},

    # ── Santander — Bucaramanga metropolitana ────────────────
    "floridablanca": {"aliases": ["floridablanca"], "department": "santander", "region": "andina"},
    "piedecuesta": {"aliases": ["piedecuesta"], "department": "santander", "region": "andina"},
    "giron": {"aliases": ["giron"], "department": "santander", "region": "andina"},
    "lebrija": {"aliases": ["lebrija"], "department": "santander", "region": "andina"},
    "los santos": {"aliases": ["los santos", "mesa de los santos"], "department": "santander", "region": "andina"},
    "barrancabermeja": {"aliases": ["barrancabermeja"], "department": "santander", "region": "andina"},

    # ── Risaralda — Eje Cafetero ─────────────────────────────
    "dosquebradas": {"aliases": ["dosquebradas"], "department": "risaralda", "region": "andina"},
    "santa rosa de cabal": {"aliases": ["santa rosa de cabal"], "department": "risaralda", "region": "andina"},

    # ── Quindío — Eje Cafetero ───────────────────────────────
    "circasia": {"aliases": ["circasia"], "department": "quindio", "region": "andina"},
    "calarca": {"aliases": ["calarca"], "department": "quindio", "region": "andina"},
    "la tebaida": {"aliases": ["la tebaida"], "department": "quindio", "region": "andina"},
    "quimbaya": {"aliases": ["quimbaya"], "department": "quindio", "region": "andina"},
    "filandia": {"aliases": ["filandia"], "department": "quindio", "region": "andina"},
    "montenegro": {"aliases": ["montenegro"], "department": "quindio", "region": "andina"},

    # ── Caldas — Eje Cafetero ────────────────────────────────
    "villamaria": {"aliases": ["villamaria"], "department": "caldas", "region": "andina"},
    "palestina": {"aliases": ["palestina"], "department": "caldas", "region": "andina"},

    # ── Tolima — Turismo / Tierra caliente ───────────────────
    "melgar": {"aliases": ["melgar"], "department": "tolima", "region": "andina"},
    "carmen de apicala": {"aliases": ["carmen de apicala"], "department": "tolima", "region": "andina"},
    "flandes": {"aliases": ["flandes"], "department": "tolima", "region": "andina"},
    "mariquita": {"aliases": ["mariquita"], "department": "tolima", "region": "andina"},
    "alvarado": {"aliases": ["alvarado"], "department": "tolima", "region": "andina"},

    # ── Boyacá ───────────────────────────────────────────────
    "villa de leyva": {"aliases": ["villa de leyva"], "department": "boyaca", "region": "andina"},
    "duitama": {"aliases": ["duitama"], "department": "boyaca", "region": "andina"},

    # ── Meta ─────────────────────────────────────────────────
    "restrepo": {"aliases": ["restrepo"], "department": "meta", "region": "orinoquia"},
    "cumaral": {"aliases": ["cumaral"], "department": "meta", "region": "orinoquia"},

    # ── Norte de Santander ───────────────────────────────────
    "los patios": {"aliases": ["los patios"], "department": "norte_de_santander", "region": "andina"},
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
        "madrid",
        "tocancipa",
        "la calera",
        "sopo",
        "tenjo",
        "tabio",
        "guasca",
        "subachoque",
        "sesquile",
        "choconta",
        "choachi",
    ],
    "turismo_cundinamarca": [
        "girardot",
        "fusagasuga",
        "anapoima",
        "la mesa",
        "villeta",
        "silvania",
        "nilo",
        "apulo",
        "ricaurte",
        "pacho",
    ],
    "valle_aburra": [
        "medellin",
        "envigado",
        "sabaneta",
        "itagui",
        "bello",
        "la estrella",
        "copacabana",
        "caldas_antioquia",
        "barbosa_antioquia",
    ],
    "oriente_antioqueno": [
        "rionegro",
        "la ceja",
        "el retiro",
        "guarne",
        "marinilla",
        "el carmen de viboral",
        "penol",
        "guatape",
        "san vicente",
    ],
    "occidente_antioqueno": [
        "sopetran",
        "santa fe de antioquia",
        "san jeronimo",
    ],
    "cali_metropolitana": [
        "cali",
        "jamundi",
        "yumbo",
        "palmira",
        "candelaria",
        "dagua",
        "la cumbre",
    ],
    "norte_valle": [
        "cartago",
        "tulua",
        "alcala",
    ],
    "barranquilla_metropolitana": [
        "barranquilla",
        "soledad",
        "puerto colombia",
        "juan de acosta",
        "tubara",
    ],
    "cartagena_metropolitana": [
        "cartagena",
    ],
    "bucaramanga_metropolitana": [
        "bucaramanga",
        "floridablanca",
        "piedecuesta",
        "giron",
        "lebrija",
        "los santos",
    ],
    "eje_cafetero": [
        "pereira",
        "manizales",
        "armenia",
        "dosquebradas",
        "santa rosa de cabal",
        "circasia",
        "calarca",
        "la tebaida",
        "quimbaya",
        "filandia",
        "montenegro",
        "villamaria",
        "palestina",
    ],
    "turismo_tolima": [
        "melgar",
        "carmen de apicala",
        "flandes",
        "mariquita",
        "alvarado",
    ],
    "cucuta_metropolitana": [
        "cucuta",
        "los patios",
    ],
    "santa_marta_metropolitana": [
        "santa marta",
    ],
    "villavicencio_metropolitana": [
        "villavicencio",
        "restrepo",
        "cumaral",
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
    "ibague_metropolitana": [
        "ibague",
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
        "villa de leyva",
        "duitama",
    ],
    "barrancabermeja_metropolitana": [
        "barrancabermeja",
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
