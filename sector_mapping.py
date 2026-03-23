"""
src/geo/sector_mapping.py
==========================
Catálogo de barrios/comunas por ciudad para asignación de
comuna_mercado y sector_mercado en el pipeline Gold y entrenamiento.

Relación con geography_catalog.py:
  geography_catalog.py  →  ciudad  →  city_token + market_token
  sector_mapping.py     →  texto   →  comuna_mercado + sector_mercado

Uso en Databricks (Spark):
    from src.geo.sector_mapping import get_spark_udfs
    assign_comuna_udf, extract_sector_udf, _ = get_spark_udfs()

Uso en Python puro (app.py, scorer.py, training):
    from src.geo.sector_mapping import assign_comuna, normalize_text

FIXES respecto a versiones anteriores:
  - Barranquilla: formato "barranquilla comuna 19 villa santos" ahora matchea
  - Cartagena: bocagrande, manga, castillogrande, crespo, torices, zona norte
  - Valle de Aburrá: envigado, rionegro, sabaneta, bello, itagui, la ceja
    buscan bajo catálogo de medellin via CITY_TO_CATALOG
  - Cali: jamundí, zonas norte/oriente/sur más completas
  - Bogotá sabana: chía, cajicá, mosquera, soacha bajo catálogo bogotá
  - Bucaramanga: floridablanca, piedecuesta, girón integrados
  - Santa Marta: rodadero, bello horizonte, taganga
  - Pereira, Manizales, Armenia: catálogos nuevos (antes 0%)
"""

import re
import unicodedata


# ══════════════════════════════════════════════════════════════════
# NORMALIZACIÓN
# ══════════════════════════════════════════════════════════════════

def normalize_text(text: str) -> str:
    if not text:
        return ""
    text = text.lower().strip()
    text = unicodedata.normalize("NFD", text)
    text = re.sub(r"[\u0300-\u036f]", "", text)
    text = re.sub(r"[^a-z0-9\s]", " ", text)
    return re.sub(r"\s+", " ", text).strip()


# ══════════════════════════════════════════════════════════════════
# CATÁLOGO DE COMUNAS / BARRIOS
# ══════════════════════════════════════════════════════════════════

COMUNA_KEYWORDS_BY_CITY: dict = {

    # ── MEDELLÍN + VALLE DE ABURRÁ ────────────────────────────────
    "medellin": {
        "el_poblado": [
            "poblado", "el poblado", "provenza", "patio bonito", "lalinde",
            "los balsos", "lomas de los balsos", "la frontera",
            "castropol", "manila", "aguacatala", "astorga", "campestre",
            "lleras", "villa carlota", "el diamante", "altos del poblado",
            "santa maria de los angeles", "alejandria poblado",
            "transversal inferior", "transversal intermedia",
        ],
        "laureles_estadio": [
            "laureles", "estadio", "bolivariana", "velodromo",
            "conquistadores", "los colores", "florida nueva",
            "carlos e restrepo", "la america", "san joaquin laureles",
            "suramericana", "naranjal", "lorena", "la castellana",
            "santa gema", "simon bolivar medellin",
        ],
        "belen_guayabal": [
            "belen", "guayabal", "rodeo alto", "la mota", "aeroparque",
            "rosetales", "rosales medellin", "loma de los bernal",
            "fatima medellin", "cristo rey medellin", "campo amor",
            "la palma medellin", "trinidad medellin",
            "altavista", "la nubia", "aguas frias", "los alpes medellin",
        ],
        "el_poblado_envigado": [
            "envigado", "la paz envigado", "zuniga", "otra parte envigado",
            "el portal envigado", "loma del escobero", "las vegas envigado",
            "alcala envigado", "milanes", "cumbres envigado",
            "ayura", "el chocho", "escobero", "pan de azucar envigado",
        ],
        "sabaneta_zona": [
            "sabaneta", "aves maria", "la doctora sabaneta",
            "restrepo naranjo", "calle larga sabaneta",
            "maria auxiliadora sabaneta",
        ],
        "itagui_zona": [
            "itagui", "ditaires", "la gloria itagui",
            "calatrava itagui", "suramerica itagui",
        ],
        "la_estrella_zona": [
            "la estrella", "pueblo viejo la estrella",
        ],
        "robledo_castilla": [
            "robledo", "castilla medellin", "caribe medellin",
            "tricentenario", "calasanz", "la pilarica",
            "floresta medellin", "la iguana", "aures",
            "doce de octubre", "picacho", "pedregal medellin",
        ],
        "aranjuez_manrique": [
            "aranjuez", "manrique", "berlin medellin", "campo valdes",
            "palos verdes medellin", "miraflores medellin",
            "san isidro medellin", "la honda", "la cruz medellin",
        ],
        "popular_santa_cruz": [
            "popular medellin", "santa cruz medellin", "andalucia medellin",
            "moscu medellin", "villa guadalupe medellin",
            "santo domingo medellin", "granizal",
        ],
        "buenos_aires_medellin": [
            "buenos aires medellin", "bombona medellin", "la milagrosa",
            "barcelona medellin", "gerona medellin",
            "el salvador medellin", "asomadera", "ocho de marzo medellin",
        ],
        "centro_medellin": [
            "candelaria medellin", "prado medellin", "sevilla medellin",
            "san diego medellin", "boston medellin",
            "villanueva medellin", "corazon de jesus",
            "guayaquil medellin",
        ],
        "bello_zona": [
            "bello antioquia", "niquia", "paris bello",
            "zamora bello", "la honda bello",
        ],
        "rionegro_zona": [
            "rionegro", "el porvenir rionegro",
            "buenos aires rionegro", "la presentacion rionegro",
        ],
        "la_ceja_zona": ["la ceja"],
        "el_retiro_zona": [
            "el retiro antioquia", "pantanillo", "fizebad",
        ],
        "guarne_zona": ["guarne"],
        "marinilla_zona": ["marinilla"],
    },

    # ── BOGOTÁ + SABANA ───────────────────────────────────────────
    "bogota": {
        "usaquen": [
            "usaquen", "santa barbara bogota", "cedritos",
            "bella suiza", "country club", "san patricio",
            "chico", "la carolina", "contador",
            "san jose de bavaria", "toberin", "bosque de pinos",
        ],
        "chapinero": [
            "chapinero", "rosales bogota", "nogal bogota",
            "quinta camacho", "el refugio bogota", "el retiro bogota",
            "antiguo country", "lago chapinero", "el nogal bogota",
        ],
        "suba": [
            "suba", "niza bogota", "colina campestre", "gratamira",
            "mazuren", "la alhambra bogota", "portales norte",
            "rincon suba", "tierra linda bogota", "casablanca suba",
        ],
        "teusaquillo_barrios_unidos": [
            "teusaquillo", "salitre bogota", "barrios unidos",
            "la esmeralda bogota", "galerias", "polo club bogota",
            "la soledad bogota", "palermo bogota", "campin", "alcazares",
        ],
        "kennedy_fontibon": [
            "kennedy bogota", "fontibon", "hayuelos",
            "modelia", "tintal", "capellania",
        ],
        "engativa": [
            "engativa", "minuto de dios", "ferias bogota",
            "boyaca real", "tabora bogota", "alamos bogota",
            "quirigua", "bachue bogota",
        ],
        "bosa_ciudad_bolivar": [
            "bosa", "ciudad bolivar", "perdomo bogota",
            "lucero bogota", "madalena bogota",
        ],
        "centro_bogota": [
            "la candelaria", "santa fe bogota", "los martires",
            "la macarena bogota", "marly bogota",
        ],
        "chia_zona": ["chia", "fonqueta", "hacienda chia"],
        "cajica_zona": ["cajica", "calahorra cajica"],
        "mosquera_zona": ["mosquera cundinamarca"],
        "soacha_zona": ["soacha", "compartir soacha"],
        "zipaquira_zona": ["zipaquira"],
        "la_calera_zona": ["la calera cundinamarca"],
        "sopo_zona": ["sopo cundinamarca"],
    },

    # ── CALI + ZONA METROPOLITANA ─────────────────────────────────
    "cali": {
        "sur_cali": [
            "ciudad jardin cali", "ciudad jardin cali",
            "pance", "el ingenio cali", "valle del lili",
            "capri cali", "melendez cali", "caney cali",
            "bochalema cali", "mayapan", "limonar",
            "pasoancho", "la hacienda cali",
            "ciudad 2000 cali", "san fernando cali",
        ],
        "oeste_cali": [
            "el penon cali", "normandia cali", "santa rita cali",
            "cristales cali", "aguacatal", "san antonio cali",
            "terron colorado", "centenario cali",
        ],
        "norte_cali": [
            "la flora cali", "versalles cali", "juanambu",
            "granada cali", "floralia cali", "salomia",
            "los andes cali", "san vicente cali",
            "brisas de los alamos",
        ],
        "oriente_cali": [
            "aguablanca", "villanueva cali", "sierra morena cali",
            "puertas del sol cali", "alfonso lopez cali",
            "el pondaje", "desepaz",
        ],
        "centro_cali": [
            "centro cali", "san nicolas cali", "obrero cali",
        ],
        "jamundi_zona": [
            "jamundi", "villa del sur jamundi",
            "haciendas jamundi", "san isidro jamundi",
        ],
        "yumbo_zona": ["yumbo"],
        "palmira_zona": ["palmira"],
    },

    # ── BARRANQUILLA + SOLEDAD ────────────────────────────────────
    # FIX: formato portal "barranquilla comuna 19 villa santos"
    "barranquilla": {
        "riomar": [
            "riomar", "el golf barranquilla", "alto prado",
            "villa country barranquilla", "buenavista barranquilla",
            "miramar barranquilla", "villa carolina barranquilla",
            "villasantos", "villa santos barranquilla",
            "ciudad del mar barranquilla",
            "el poblado barranquilla",
            "boston barranquilla", "prado barranquilla",
            "paraiso barranquilla",
            # Patrones de comunas del portal
            "comuna 19 villa santos", "comuna 19 nuevo horizonte",
            "comuna 19 ciudad jardin", "comuna 19 el tabor",
            "comuna 19 san vicente", "comuna 19 altamira",
            "comuna 19 andalucia", "comuna 19 el limoncito",
            "ciudad jardin barranquilla",
            "el tabor barranquilla", "alameda del rio",
            "villa campestre barranquilla", "alameda barranquilla",
        ],
        "norte_centro": [
            "el prado barranquilla", "bellavista barranquilla",
            "san roque barranquilla", "barrio abajo barranquilla",
            "la concepcion barranquilla",
            "comuna 10 rosario", "comuna 10 centro",
            "comuna 12 ub altos de parque",
            "comuna 13 san isidro",
        ],
        "sur_occidente": [
            "el porvenir barranquilla", "las delicias barranquilla",
            "la castellana barranquilla", "la cumbre barranquilla",
            "los alpes barranquilla", "santa monica barranquilla",
            "granadillo barranquilla",
            "comuna 18 el porvenir", "comuna 17 colombia",
            "comuna 17 el recreo", "comuna 17 las delicias",
            "comuna 16 batallon infanteria",
            "comuna 16 la concepcion", "comuna 16 paraiso",
            "comuna 4 los angeles", "comuna 4 villa rosario",
            "comuna 1 cr el lago",
        ],
        "soledad_zona": [
            "soledad barranquilla", "soledad atlantico",
        ],
        "puerto_colombia_zona": [
            "puerto colombia", "pradomar", "salgar",
        ],
    },

    # ── CARTAGENA ─────────────────────────────────────────────────
    "cartagena": {
        "bocagrande_castillogrande": [
            "bocagrande", "castillogrande", "laguito", "el laguito",
            "cartagena boca grande", "cartagena castillo grande",
            "zona turistica bocagrande",
            "cartagena comuna 1 boca grande",
            "cartagena comuna 1 castillo grande",
        ],
        "manga_crespo": [
            "manga cartagena", "pie de la popa", "la popa cartagena",
            "crespo cartagena", "zona residencial crespo",
            "zona turistica manga", "cabrero cartagena",
            "cartagena comuna 1 manga",
            "cartagena comuna 1 pie de la popa",
            "cartagena comuna 1 crespo",
            "cartagena comuna 1 el laguito",
        ],
        "historico": [
            "getsemani", "centro historico cartagena",
            "san diego cartagena", "torices cartagena",
            "cartagena historica y del caribe norte",
            "cartagena comuna 2 torices",
        ],
        "zona_norte": [
            "zona norte cartagena", "la boquilla",
            "manzanillo del mar", "serena del mar",
            "karibana", "zona norte manzanillo del mar",
            "zona norte la boquilla", "zona norte serena del mar",
            "zona norte cielo mar", "playa escondida",
            "baru", "cartagena baru",
            "zona norte via anillo vial",
        ],
        "norte_residencial": [
            "el bosque cartagena", "ternera cartagena",
            "cartagena comuna 10 el bosque",
            "cartagena comuna 13 el recreo",
            "cartagena comuna 13 providencia",
            "cartagena comuna 12 el socorro",
        ],
    },

    # ── BUCARAMANGA + ÁREA METROPOLITANA ─────────────────────────
    "bucaramanga": {
        "cabecera": [
            "cabecera bucaramanga", "sotomayor bucaramanga",
            "pan de azucar bucaramanga", "conucos",
            "terrazas bucaramanga", "altos de cabecera",
            "los cedros bucaramanga", "lagos bucaramanga",
        ],
        "centro_bucaramanga": [
            "centro bucaramanga", "san alonso",
            "bolivar bucaramanga",
        ],
        "provenza_bucaramanga": [
            "provenza bucaramanga", "diamante bucaramanga",
            "san luis bucaramanga", "fontana bucaramanga",
            "estoraques", "ciudadela bucaramanga",
        ],
        "floridablanca_zona": [
            "floridablanca", "ciudadela real de minas",
            "villabel floridablanca",
        ],
        "piedecuesta_zona": ["piedecuesta"],
        "giron_zona": ["giron santander"],
    },

    # ── SANTA MARTA ───────────────────────────────────────────────
    "santa marta": {
        "rodadero": [
            "rodadero", "el rodadero", "pozos colorados",
            "bello horizonte santa marta", "playa salguero",
            "rodadero sur",
        ],
        "centro_santa_marta": [
            "centro santa marta", "miramar santa marta",
            "el prado santa marta", "mamatoco",
        ],
        "norte_santa_marta": [
            "taganga", "playa dormida", "sierra nevada santa marta",
            "bahia concha",
        ],
        "bello_horizonte_zona": [
            "bello horizonte", "las americas santa marta",
            "villa del mar santa marta",
        ],
    },

    # ── PEREIRA (NUEVO) ───────────────────────────────────────────
    "pereira": {
        "pinares_cuba": [
            "pinares pereira", "alamos pereira",
            "cuba pereira", "el jardin pereira",
            "av 30 de agosto pereira",
        ],
        "laureles_pereira": [
            "laureles pereira", "villa del prado pereira",
            "el poblado pereira", "belmonte pereira",
        ],
        "centro_pereira": [
            "centro pereira", "circunvalar pereira",
        ],
        "cerritos_zona": [
            "cerritos pereira", "la virginia pereira",
        ],
        "dosquebradas_zona": [
            "dosquebradas", "la pradera dosquebradas",
        ],
    },

    # ── MANIZALES (NUEVO) ─────────────────────────────────────────
    "manizales": {
        "cable_millan": [
            "el cable manizales", "millan manizales",
            "los rosales manizales", "la enea manizales",
            "belEN manizales",
        ],
        "palogrande_chipre": [
            "palogrande", "chipre manizales",
            "san jorge manizales", "bosques del norte manizales",
        ],
        "centro_manizales": ["centro manizales"],
        "villamaria_zona": ["villamaria"],
    },

    # ── ARMENIA (NUEVO) ───────────────────────────────────────────
    "armenia": {
        "el_bosque_armenia": [
            "el bosque armenia", "la castellana armenia",
            "los cedros armenia", "la miranda armenia",
            "el caimo armenia",
        ],
        "centro_armenia": ["centro armenia", "la clarita armenia"],
        "calarca_zona": ["calarca", "la tebaida", "quimbaya"],
    },

    # ── IBAGUÉ ────────────────────────────────────────────────────
    "ibague": {
        "ambala_picaleña": [
            "ambala ibague", "picaleña", "el jordan ibague",
        ],
        "centro_ibague": ["centro ibague"],
        "calambeo_zona": ["calambeo ibague"],
    },

}


# ══════════════════════════════════════════════════════════════════
# CITY_TO_CATALOG — mapea city_token al catálogo de comunas
# Ciudades del Valle de Aburrá y Sabana de Bogotá usan el
# catálogo de su ciudad ancla.
# ══════════════════════════════════════════════════════════════════

CITY_TO_CATALOG: dict = {
    # Valle de Aburrá
    "medellin": "medellin", "envigado": "medellin",
    "sabaneta": "medellin", "itagui": "medellin",
    "bello": "medellin", "la estrella": "medellin",
    "copacabana": "medellin", "rionegro": "medellin",
    "la ceja": "medellin", "el retiro": "medellin",
    "guarne": "medellin", "marinilla": "medellin",
    # Sabana Bogotá
    "bogota": "bogota", "chia": "bogota",
    "cajica": "bogota", "mosquera": "bogota",
    "cota": "bogota", "soacha": "bogota",
    "zipaquira": "bogota", "la calera": "bogota",
    "sopo": "bogota", "tocancipa": "bogota",
    # Cali metropolitana
    "cali": "cali", "jamundi": "cali",
    "yumbo": "cali", "palmira": "cali",
    "candelaria": "cali",
    # Barranquilla metropolitana
    "barranquilla": "barranquilla",
    "soledad": "barranquilla",
    "puerto colombia": "barranquilla",
    # Bucaramanga metropolitana
    "bucaramanga": "bucaramanga",
    "floridablanca": "bucaramanga",
    "piedecuesta": "bucaramanga",
    "giron": "bucaramanga",
    # Ciudades independientes
    "cartagena": "cartagena",
    "santa marta": "santa marta",
    "pereira": "pereira",
    "manizales": "manizales",
    "armenia": "armenia",
    "ibague": "ibague",
}


# ══════════════════════════════════════════════════════════════════
# STOPWORDS
# ══════════════════════════════════════════════════════════════════

SECTOR_STOPWORDS: set = {
    "apartamento", "apartaestudio", "casa", "lote", "finca", "oficina",
    "local", "comercial", "venta", "arriendo", "colombia", "sector",
    "barrio", "zona", "urbanizacion", "unidad", "conjunto", "edificio",
    "torre", "apto", "piso", "norte", "sur", "oriente", "occidente",
    "cerca", "frente", "calle", "carrera", "avenida", "transversal",
    "diagonal", "via", "autopista", "vereda", "corregimiento",
}

CITY_TOKENS: set = {
    "bogota", "medellin", "cali", "barranquilla", "cartagena",
    "bucaramanga", "pereira", "manizales", "armenia", "cucuta",
    "ibague", "santa", "marta", "envigado", "sabaneta", "itagui",
    "bello", "rionegro", "chia", "cajica", "mosquera", "soacha",
    "zipaquira", "jamundi", "yumbo", "palmira", "floridablanca",
    "piedecuesta", "giron", "soledad", "colombia", "antioquia",
    "cundinamarca", "atlantico", "bolivar", "santander",
    "risaralda", "caldas", "quindio", "tolima",
}


# ══════════════════════════════════════════════════════════════════
# FUNCIONES PRINCIPALES
# ══════════════════════════════════════════════════════════════════

def assign_comuna(city_token: str, ubicacion_limpia: str) -> str:
    """
    Asigna la comuna/barrio dado city_token y texto de ubicación.
    Usa CITY_TO_CATALOG para resolver ciudades del Valle de Aburrá, Sabana, etc.
    """
    catalog_key = CITY_TO_CATALOG.get(normalize_text(city_token), normalize_text(city_token))
    city_map = COMUNA_KEYWORDS_BY_CITY.get(catalog_key, {})
    if not city_map:
        return "comuna_otra"

    location = normalize_text(ubicacion_limpia)
    if not location:
        return "comuna_otra"

    location_tokens = set(location.split())

    for comuna_name, keywords in city_map.items():
        for keyword in keywords:
            if keyword in location:
                return comuna_name
            kw_tokens = set(keyword.split())
            if len(kw_tokens) > 1 and kw_tokens.issubset(location_tokens):
                return comuna_name

    return "comuna_otra"


def extract_sector_mercado(city_token: str, comuna_mercado: str, ubicacion_limpia: str) -> str:
    """Extrae el subsector dentro de la comuna."""
    location = normalize_text(ubicacion_limpia)
    if not location:
        return comuna_mercado if comuna_mercado != "comuna_otra" else "sector_otra"

    catalog_key = CITY_TO_CATALOG.get(normalize_text(city_token), normalize_text(city_token))
    city_map = COMUNA_KEYWORDS_BY_CITY.get(catalog_key, {})

    matched_keywords: set = set()
    best_keyword = ""
    location_tokens = set(location.split())

    for keywords in city_map.values():
        for keyword in keywords:
            hit = keyword in location
            if not hit:
                kw_tokens = set(keyword.split())
                hit = len(kw_tokens) > 1 and kw_tokens.issubset(location_tokens)
            if hit:
                matched_keywords.update(keyword.split())
                if len(keyword) > len(best_keyword):
                    best_keyword = keyword

    sector_tokens = [
        t for t in location.split()
        if t not in SECTOR_STOPWORDS
        and t not in CITY_TOKENS
        and t not in matched_keywords
        and not t.isdigit()
    ][:3]

    sector_name = " ".join(sector_tokens).strip()
    if not sector_name and best_keyword:
        sector_name = best_keyword
    elif not sector_name and comuna_mercado != "comuna_otra":
        sector_name = comuna_mercado.replace("_", " ")
    return sector_name or "sector_otra"


# ══════════════════════════════════════════════════════════════════
# SPARK UDFs
# ══════════════════════════════════════════════════════════════════

def get_spark_udfs():
    """
    Retorna (assign_comuna_udf, extract_sector_udf, canonicalize_city_udf)
    para usar en notebooks de Databricks.
    """
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def _canonicalize_city(raw: str) -> str:
        val = normalize_text(raw or "")
        return CITY_TO_CATALOG.get(val, val) if val else "otra_ciudad"

    return (
        udf(assign_comuna, StringType()),
        udf(extract_sector_mercado, StringType()),
        udf(_canonicalize_city, StringType()),
    )