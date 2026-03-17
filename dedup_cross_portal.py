"""
Módulo de Deduplicación Cross-Portal — Lakehouse Inmobiliario Colombia

Identifica el mismo inmueble publicado en múltiples portales
y convierte duplicados en inteligencia de precios.

Pipeline: Silver (normalizado) → Dedup Cross-Portal → Gold (limpio + inteligencia)
"""

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, ArrayType
import unicodedata
import re


def _safe_cache(df: DataFrame) -> DataFrame:
    """Cache a DataFrame if the cluster supports it; no-op on serverless."""
    try:
        return df.cache()
    except Exception:
        return df


# ─────────────────────────────────────────────────────────────────
# 1. NORMALIZACIÓN DE UBICACIÓN
# ─────────────────────────────────────────────────────────────────

def _remove_accents(text):
    if text is None:
        return None
    nfkd = unicodedata.normalize("NFKD", text)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _normalize_location(text):
    if text is None:
        return None
    text = _remove_accents(text.lower().strip())
    for noise in ["d.c.", "d.c", "dc", "colombia", "departamento", "municipio"]:
        text = text.replace(noise, "")
    text = re.sub(r"[,\-–—/|·•()]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


normalize_location_udf = F.udf(_normalize_location, StringType())


def _extract_city_token(normalized_location):
    if normalized_location is None:
        return "unknown"
    ciudades = {
        "bogota", "medellin", "cali", "barranquilla", "cartagena",
        "bucaramanga", "pereira", "manizales", "cucuta", "ibague",
        "santa marta", "villavicencio", "pasto", "monteria", "neiva",
        "armenia", "popayan", "valledupar", "sincelejo", "tunja",
        "envigado", "sabaneta", "itagui", "bello", "soacha",
        "chia", "zipaquira", "cajica", "cota", "funza",
        "mosquera", "tocancipa", "la calera", "sopo",
        "rionegro", "girardot", "floridablanca", "piedecuesta",
    }
    for city in ciudades:
        if city in normalized_location:
            return city
    tokens = normalized_location.split()
    return tokens[-1] if tokens else "unknown"


extract_city_udf = F.udf(_extract_city_token, StringType())


def _location_tokens(normalized_location):
    if normalized_location is None:
        return []
    return [t for t in normalized_location.split() if len(t) > 2]


location_tokens_udf = F.udf(_location_tokens, ArrayType(StringType()))


# ─────────────────────────────────────────────────────────────────
# 2. PREPARACIÓN PARA MATCHING
# ─────────────────────────────────────────────────────────────────

def prepare_for_matching(df_silver: DataFrame) -> DataFrame:
    return (
        df_silver
        .withColumn("ubicacion_norm", normalize_location_udf(F.col("ubicacion_raw")))
        .withColumn("city_token", extract_city_udf(F.col("ubicacion_norm")))
        .withColumn("location_tokens", location_tokens_udf(F.col("ubicacion_norm")))
        .withColumn("data_completeness",
            F.when(F.col("area_m2").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("habitaciones").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("banos").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("garajes").isNotNull(), F.lit(1)).otherwise(F.lit(0)) +
            F.when(F.col("titulo").isNotNull() & (F.length(F.col("titulo")) > 5), F.lit(1)).otherwise(F.lit(0))
        )
    )


# ─────────────────────────────────────────────────────────────────
# 3. BLOCKING + MATCHING
# ─────────────────────────────────────────────────────────────────

def find_cross_portal_matches(df_prepared: DataFrame,
                               area_tolerance: float = 0.10,
                               location_sim_threshold: float = 0.5,
                               max_price_ratio: float = 1.25) -> DataFrame:
    a = df_prepared.alias("a")
    b = df_prepared.alias("b")

    pairs = (
        a.join(b, on=[
            F.col("a.city_token") == F.col("b.city_token"),
            F.col("a.habitaciones") == F.col("b.habitaciones"),
        ], how="inner")
        .filter(
            (F.col("a.fuente") < F.col("b.fuente")) &
            (F.col("a.area_m2").isNotNull() | F.col("b.area_m2").isNotNull())
        )
    )

    # Filtro de área
    pairs = pairs.filter(
        F.when(
            F.col("a.area_m2").isNotNull() & F.col("b.area_m2").isNotNull(),
            (F.abs(F.col("a.area_m2") - F.col("b.area_m2")) /
             F.greatest(F.col("a.area_m2"), F.col("b.area_m2"))) <= area_tolerance
        ).otherwise(F.lit(True))
    )

    # Filtro de baños
    pairs = pairs.filter(
        F.when(
            F.col("a.banos").isNotNull() & F.col("b.banos").isNotNull(),
            F.abs(F.col("a.banos") - F.col("b.banos")) <= 1
        ).otherwise(F.lit(True))
    )

    # Filtro de precio
    pairs = pairs.filter(
        F.when(
            F.col("a.precio_num").isNotNull() & F.col("b.precio_num").isNotNull() &
            (F.col("a.precio_num") > 0) & (F.col("b.precio_num") > 0),
            (F.greatest(F.col("a.precio_num"), F.col("b.precio_num")) /
             F.least(F.col("a.precio_num"), F.col("b.precio_num"))) <= max_price_ratio
        ).otherwise(F.lit(True))
    )

    # Jaccard similarity
    pairs = (
        pairs
        .withColumn("tokens_intersect",
            F.size(F.array_intersect(F.col("a.location_tokens"), F.col("b.location_tokens"))))
        .withColumn("tokens_union",
            F.size(F.array_union(F.col("a.location_tokens"), F.col("b.location_tokens"))))
        .withColumn("jaccard_sim",
            F.when(F.col("tokens_union") > 0,
                   F.col("tokens_intersect") / F.col("tokens_union"))
            .otherwise(F.lit(0.0))
        )
        .filter(F.col("jaccard_sim") >= location_sim_threshold)
    )

    # Score compuesto
    pairs_final = (
        pairs
        .withColumn("area_sim",
            F.when(
                F.col("a.area_m2").isNotNull() & F.col("b.area_m2").isNotNull(),
                F.lit(1.0) - (F.abs(F.col("a.area_m2") - F.col("b.area_m2")) /
                              F.greatest(F.col("a.area_m2"), F.col("b.area_m2")))
            ).otherwise(F.lit(0.5))
        )
        .withColumn("price_sim",
            F.when(
                F.col("a.precio_num").isNotNull() & F.col("b.precio_num").isNotNull() &
                (F.col("a.precio_num") > 0) & (F.col("b.precio_num") > 0),
                F.lit(1.0) - (F.abs(F.col("a.precio_num") - F.col("b.precio_num")).cast("double") /
                              F.greatest(F.col("a.precio_num"), F.col("b.precio_num")).cast("double"))
            ).otherwise(F.lit(0.5))
        )
        .withColumn("match_score",
            F.col("jaccard_sim") * 0.40 +
            F.col("area_sim") * 0.35 +
            F.col("price_sim") * 0.25
        )
        .select(
            F.col("a.id_original").alias("id_a"),
            F.col("a.fuente").alias("fuente_a"),
            F.col("a.ubicacion_raw").alias("ubicacion_a"),
            F.col("a.precio_num").alias("precio_a"),
            F.col("a.area_m2").alias("area_a"),
            F.col("b.id_original").alias("id_b"),
            F.col("b.fuente").alias("fuente_b"),
            F.col("b.ubicacion_raw").alias("ubicacion_b"),
            F.col("b.precio_num").alias("precio_b"),
            F.col("b.area_m2").alias("area_b"),
            F.col("jaccard_sim"),
            F.col("area_sim"),
            F.col("price_sim"),
            F.col("match_score"),
        )
        .filter(F.col("match_score") >= 0.60)
    )

    return pairs_final


# ─────────────────────────────────────────────────────────────────
# 4. COMPONENTES CONECTADOS (UNION-FIND EN PYTHON — serverless-safe)
# ─────────────────────────────────────────────────────────────────

class _UnionFind:
    """Lightweight Union-Find with path compression + union by rank."""
    def __init__(self):
        self.parent = {}
        self.rank = {}

    def find(self, x):
        if x not in self.parent:
            self.parent[x] = x
            self.rank[x] = 0
        while self.parent[x] != x:
            self.parent[x] = self.parent[self.parent[x]]  # path compression
            x = self.parent[x]
        return x

    def union(self, a, b):
        ra, rb = self.find(a), self.find(b)
        if ra == rb:
            return
        if self.rank[ra] < self.rank[rb]:
            ra, rb = rb, ra
        self.parent[rb] = ra
        if self.rank[ra] == self.rank[rb]:
            self.rank[ra] += 1


def assign_property_groups(df_prepared: DataFrame,
                           df_pairs: DataFrame) -> DataFrame:
    # Collect edges to driver — typically <100K pairs, trivial for Python
    edges = df_pairs.select(
        F.concat_ws("||", F.col("fuente_a"), F.col("id_a")).alias("src"),
        F.concat_ws("||", F.col("fuente_b"), F.col("id_b")).alias("dst"),
    ).collect()

    uf = _UnionFind()
    for row in edges:
        uf.union(row["src"], row["dst"])

    # Build mapping: node_id → group_label (root of its component)
    group_map = {node: uf.find(node) for node in uf.parent}
    mapping_rows = [(node, group) for node, group in group_map.items()]

    spark = df_prepared.sparkSession
    df_labels = spark.createDataFrame(mapping_rows, ["node_id", "group_label"])

    return (
        df_prepared
        .withColumn("node_id", F.concat_ws("||", F.col("fuente"), F.col("id_original")))
        .join(df_labels, "node_id", "left")
        .withColumn("property_group_id", F.coalesce(F.col("group_label"), F.col("node_id")))
        .drop("node_id", "group_label")
    )


# ─────────────────────────────────────────────────────────────────
# 5. SELECCIÓN DE REPRESENTANTE PARA ML
# ─────────────────────────────────────────────────────────────────

def select_ml_representative(df_grouped: DataFrame) -> DataFrame:
    group_stats = (
        df_grouped
        .groupBy("property_group_id")
        .agg(
            F.count("*").alias("num_portales"),
            F.collect_set("fuente").alias("portales"),
            F.expr("percentile_approx(precio_num, 0.5)").alias("precio_mediano_grupo"),
            F.min("precio_num").alias("precio_min_grupo"),
            F.max("precio_num").alias("precio_max_grupo"),
        )
    )

    w = Window.partitionBy("property_group_id").orderBy(
        F.desc("data_completeness"), F.desc("fecha_extraccion")
    )

    return (
        df_grouped
        .withColumn("_rank", F.row_number().over(w))
        .filter(F.col("_rank") == 1)
        .drop("_rank")
        .join(group_stats, "property_group_id", "left")
        .withColumn("precio_original_portal", F.col("precio_num"))
        .withColumn("precio_num",
            F.when(F.col("num_portales") > 1, F.col("precio_mediano_grupo"))
            .otherwise(F.col("precio_num"))
        )
    )


# ─────────────────────────────────────────────────────────────────
# 6. INTELIGENCIA DE PRECIOS CROSS-PORTAL
# ─────────────────────────────────────────────────────────────────

def build_price_intelligence(df_grouped: DataFrame):
    multi_portal = (
        df_grouped
        .groupBy("property_group_id")
        .agg(F.countDistinct("fuente").alias("n_portales"))
        .filter(F.col("n_portales") > 1)
        .select("property_group_id")
    )

    df_multi = df_grouped.join(multi_portal, "property_group_id", "inner")

    df_prices_by_portal = df_multi.select("property_group_id", "fuente", "precio_num", "url")

    group_intelligence = (
        df_multi
        .groupBy("property_group_id")
        .agg(
            F.count("*").alias("num_portales"),
            F.collect_set("fuente").alias("portales_disponibles"),
            F.min("precio_num").alias("precio_minimo"),
            F.max("precio_num").alias("precio_maximo"),
            F.expr("percentile_approx(precio_num, 0.5)").alias("precio_mediano"),
            F.round(F.stddev("precio_num"), 0).alias("precio_stddev"),
        )
        .withColumn("ahorro_potencial", F.col("precio_maximo") - F.col("precio_minimo"))
        .withColumn("ahorro_pct",
            F.round((F.col("precio_maximo") - F.col("precio_minimo")) / F.col("precio_maximo") * 100, 1))
    )

    w_min = Window.partitionBy("property_group_id").orderBy(F.asc("precio_num"))
    w_max = Window.partitionBy("property_group_id").orderBy(F.desc("precio_num"))

    portal_cheapest = (
        df_multi.withColumn("_r", F.row_number().over(w_min)).filter(F.col("_r") == 1)
        .select("property_group_id",
                F.col("fuente").alias("portal_mas_barato"),
                F.col("url").alias("url_mas_barato"),
                F.col("precio_num").alias("precio_portal_barato"))
    )

    portal_expensive = (
        df_multi.withColumn("_r", F.row_number().over(w_max)).filter(F.col("_r") == 1)
        .select("property_group_id",
                F.col("fuente").alias("portal_mas_caro"),
                F.col("url").alias("url_mas_caro"),
                F.col("precio_num").alias("precio_portal_caro"))
    )

    w_best = Window.partitionBy("property_group_id").orderBy(
        F.desc("data_completeness"), F.desc("fecha_extraccion")
    )
    representative_info = (
        df_multi.withColumn("_r", F.row_number().over(w_best)).filter(F.col("_r") == 1)
        .select("property_group_id",
                F.col("titulo").alias("titulo_inmueble"),
                "ubicacion_raw", "area_m2", "habitaciones", "banos",
                "garajes", "tipo_inmueble", "estado_inmueble")
    )

    df_intelligence = (
        group_intelligence
        .join(portal_cheapest, "property_group_id", "left")
        .join(portal_expensive, "property_group_id", "left")
        .join(representative_info, "property_group_id", "left")
        .orderBy(F.desc("ahorro_potencial"))
    )

    df_detail = (
        df_prices_by_portal
        .select("property_group_id", "fuente", "precio_num", "url")
        .orderBy("property_group_id", "precio_num")
    )

    return df_intelligence, df_detail


# ─────────────────────────────────────────────────────────────────
# 7. ORQUESTADOR PRINCIPAL
# ─────────────────────────────────────────────────────────────────

def run_cross_portal_dedup(df_silver: DataFrame,
                           area_tolerance: float = 0.10,
                           location_sim_threshold: float = 0.50,
                           max_price_ratio: float = 1.25):
    print("=" * 65)
    print("  DEDUP CROSS-PORTAL — Inicio")
    print("=" * 65)

    print("\n[1/5] Preparando datos para matching...")
    df_prepared = prepare_for_matching(df_silver)
    total_records = df_prepared.count()
    total_portals = df_prepared.select("fuente").distinct().count()
    print(f"      → {total_records:,} registros de {total_portals} portales")

    print("\n[2/5] Buscando matches cross-portal...")
    df_pairs = find_cross_portal_matches(
        df_prepared,
        area_tolerance=area_tolerance,
        location_sim_threshold=location_sim_threshold,
        max_price_ratio=max_price_ratio,
    )
    df_pairs = _safe_cache(df_pairs)
    total_pairs = df_pairs.count()
    print(f"      → {total_pairs:,} pares identificados")

    print("\n[3/5] Asignando grupos (componentes conectados)...")
    df_grouped = assign_property_groups(df_prepared, df_pairs)
    df_grouped = _safe_cache(df_grouped)
    n_groups = df_grouped.select("property_group_id").distinct().count()
    n_multi = (
        df_grouped.groupBy("property_group_id")
        .agg(F.countDistinct("fuente").alias("n"))
        .filter(F.col("n") > 1)
        .count()
    )
    print(f"      → {n_groups:,} inmuebles únicos ({n_multi:,} en múltiples portales)")
    print(f"      → Reducción: {total_records:,} → {n_groups:,} "
          f"({(1 - n_groups/max(total_records,1))*100:.1f}% duplicados)")

    print("\n[4/5] Seleccionando representantes para ML...")
    df_ml_clean = select_ml_representative(df_grouped)
    ml_count = df_ml_clean.count()
    print(f"      → {ml_count:,} registros limpios para entrenamiento")

    print("\n[5/5] Generando inteligencia de precios cross-portal...")
    df_intelligence, df_price_detail = build_price_intelligence(df_grouped)
    intel_count = df_intelligence.count()

    if intel_count > 0:
        avg_savings = df_intelligence.agg(
            F.avg("ahorro_potencial").alias("avg"),
            F.max("ahorro_potencial").alias("max"),
        ).first()
        print(f"      → {intel_count:,} inmuebles con precio en múltiples portales")
        print(f"      → Ahorro promedio: ${avg_savings['avg']:,.0f} COP")
        print(f"      → Ahorro máximo:   ${avg_savings['max']:,.0f} COP")
    else:
        print("      → No se encontraron inmuebles en múltiples portales")

    stats = {
        "total_records_input": total_records,
        "total_portals": total_portals,
        "total_pairs_matched": total_pairs,
        "unique_properties": n_groups,
        "multi_portal_properties": n_multi,
        "ml_clean_records": ml_count,
        "dedup_reduction_pct": round((1 - n_groups / max(total_records, 1)) * 100, 1),
        "price_intelligence_count": intel_count,
    }

    print("\n" + "=" * 65)
    print("  DEDUP CROSS-PORTAL — Completo ✓")
    print("=" * 65)

    return {
        "df_ml_clean": df_ml_clean,
        "df_intelligence": df_intelligence,
        "df_price_detail": df_price_detail,
        "df_pairs": df_pairs,
        "df_grouped": df_grouped,
        "stats": stats,
    }
