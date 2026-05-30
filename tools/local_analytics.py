"""
Analítica local sobre Bronze (S3 → pandas).
Lee parquets de S3, normaliza esquemas heterogéneos,
y produce un diagnóstico integral listo para decidir qué subir a Databricks.

Requisitos:  pip install pyarrow s3fs pandas tabulate
"""
import json, re, unicodedata, warnings
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq
import s3fs

warnings.filterwarnings("ignore")

# ── Credenciales ──────────────────────────────────────────────
with open(Path(__file__).parent / "aws_secrets.json") as f:
    _cfg = json.load(f)

BUCKET = _cfg.get("bucket_name", "bronce-scrap-date")
_fs = s3fs.S3FileSystem(
    key=_cfg["aws_access_key"],
    secret=_cfg["aws_secret_key"],
)

FUENTES = [
    "bancolombia_tu360",
    "ciencuadras",
    "ciencuadras_nuevo",
    "ciencuadras_usado",
    "facebook",
    "fincaraiz",
    "mercadolibre",
    "metrocuadrado",
    "properati",
]

MIN_PRICE = 20_000_000
MAX_PRICE = 20_000_000_000

# ── Catálogo geográfico (reutiliza el .py del repo) ──────────
from geography_catalog import (
    CITY_ALIAS_TO_CANONICAL,
    CITY_TO_DEPARTMENT,
    CITY_TO_REGION,
    CITY_TO_MARKET,
    SORTED_CITY_ALIASES,
)


# ═══════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════
def _normalize_text(s: str | None) -> str:
    """Quita acentos, baja a minúscula, colapsa espacios."""
    if not s or not isinstance(s, str):
        return ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", s).strip().lower()


def _extract_city(ubicacion_norm: str) -> str:
    """Busca la primera ciudad conocida en el texto normalizado."""
    for alias in SORTED_CITY_ALIASES:
        if alias in ubicacion_norm:
            return CITY_ALIAS_TO_CANONICAL[alias]
    return "otra_ciudad"


def _parse_price(val) -> float | None:
    """Intenta convertir cualquier representación de precio a número."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val) if val > 0 else None
    s = str(val).replace("$", "").replace(",", "").replace(".", "").strip()
    # Si el string original usaba puntos como separadores de miles (ej "350.000.000")
    # strip ya los quitó.  Try integer parse.
    m = re.search(r"(\d{5,})", s)
    if m:
        return float(m.group(1))
    return None


def _parse_numeric(val) -> float | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    m = re.search(r"(\d+\.?\d*)", str(val))
    return float(m.group(1)) if m else None


def _parse_int(val) -> int | None:
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return int(val) if val >= 0 else None
    m = re.search(r"(\d+)", str(val))
    return int(m.group(1)) if m else None


TIPO_MAP = {
    "casa": "casa",
    "apartamento": "apartamento",
    "lote": "lote",
    "finca": "finca",
    "oficina": "oficina",
    "local": "local_comercial",
    "bodega": "bodega",
}


def _normalize_tipo(raw: str | None) -> str:
    if not raw:
        return "otro"
    t = _normalize_text(raw)
    for k, v in TIPO_MAP.items():
        if k in t:
            return v
    return "otro"


# ═══════════════════════════════════════════════════════════════
# 1. LEER TODAS LAS TABLAS DELTA (via Delta log + parquet)
# ═══════════════════════════════════════════════════════════════
print("=" * 70)
print("LEYENDO BRONZE COMPLETO DESDE S3 (Delta log → parquet)")
print("=" * 70)


def _read_delta_via_log(fuente: str) -> pd.DataFrame:
    """Lee una tabla Delta parseando el log transaccional."""
    prefix = f"{BUCKET}/bronze/{fuente}"
    log_dir = f"{prefix}/_delta_log"

    # Listar JSONs del log
    log_files = sorted([
        f for f in _fs.ls(log_dir, detail=False)
        if f.endswith(".json")
    ])

    # Replay del log: mantener set de archivos activos
    active_files: set[str] = set()
    for log_file in log_files:
        with _fs.open(log_file, "r", encoding="utf-8") as fh:
            for line in fh:
                entry = json.loads(line)
                if "add" in entry:
                    active_files.add(entry["add"]["path"])
                if "remove" in entry:
                    active_files.discard(entry["remove"]["path"])

    if not active_files:
        return pd.DataFrame()

    # Leer solo los parquets activos
    frames = []
    for pf in active_files:
        full_path = f"{prefix}/{pf}"
        try:
            tbl = pq.read_table(full_path, filesystem=_fs)
            frames.append(tbl.to_pandas())
        except Exception:
            pass

    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


raw_frames: dict[str, pd.DataFrame] = {}
for fuente in FUENTES:
    try:
        df = _read_delta_via_log(fuente)
        raw_frames[fuente] = df
        print(f"  ✅ {fuente:25s}  {len(df):>6,} filas   {len(df.columns):>2} cols")
    except Exception as exc:
        print(f"  ❌ {fuente:25s}  error: {exc}")

total_raw = sum(len(df) for df in raw_frames.values())
print(f"\n  TOTAL RAW: {total_raw:,} filas\n")

# ═══════════════════════════════════════════════════════════════
# 2. NORMALIZAR ESQUEMAS → DataFrame unificado
# ═══════════════════════════════════════════════════════════════

# Mapeo de columnas heterogéneas → nombres canónicos
COL_MAP_ID = ["id_inmueble", "id_original", "id", "property_id", "listing_id"]
COL_MAP_TITLE = ["titulo", "title"]
COL_MAP_PRICE_NUM = ["precio_num"]
COL_MAP_PRICE_STR = ["precio", "price"]
COL_MAP_LOCATION = ["ubicacion", "location", "address", "direccion", "municipio_texto"]
COL_MAP_AREA = ["area", "area_m2"]
COL_MAP_ROOMS = ["habitaciones", "rooms", "bedrooms"]
COL_MAP_BATHS = ["banos", "bathrooms", "baths"]
COL_MAP_PARKING = ["garajes", "parking", "garages"]
COL_MAP_TYPE = ["tipo_inmueble", "property_type", "category"]
COL_MAP_URL = ["url"]
COL_MAP_DATE = ["fecha_extraccion", "extracted_at"]


def _first_existing(df: pd.DataFrame, candidates: list[str]):
    """Retorna la primera columna que exista y no sea totalmente nula."""
    for c in candidates:
        if c in df.columns and df[c].notna().any():
            return df[c]
    return pd.Series([None] * len(df), dtype=object)


def normalizar_local(fuente: str, df_raw: pd.DataFrame) -> pd.DataFrame:
    """Replica la lógica de normalizar_fuente() de Silver, pero en pandas."""
    out = pd.DataFrame()
    out["id_original"] = _first_existing(df_raw, COL_MAP_ID).astype(str)
    out["titulo"] = _first_existing(df_raw, COL_MAP_TITLE)
    out["fuente"] = fuente

    # Precio: preferir numérico, fallback a parseo de string
    precio_num_series = _first_existing(df_raw, COL_MAP_PRICE_NUM)
    precio_str_series = _first_existing(df_raw, COL_MAP_PRICE_STR)
    out["precio_num"] = precio_num_series.combine_first(
        precio_str_series.apply(_parse_price)
    ).apply(lambda x: _parse_price(x))

    # Ubicación
    out["ubicacion_raw"] = _first_existing(df_raw, COL_MAP_LOCATION)
    out["ubicacion_norm"] = out["ubicacion_raw"].apply(_normalize_text)
    out["city_token"] = out["ubicacion_norm"].apply(_extract_city)
    out["departamento_token"] = out["city_token"].map(CITY_TO_DEPARTMENT).fillna("otro")
    out["region_token"] = out["city_token"].map(CITY_TO_REGION).fillna("otra")
    out["market_token"] = out["city_token"].map(CITY_TO_MARKET).fillna("mercado_otro")

    # Numéricos
    out["area_m2"] = _first_existing(df_raw, COL_MAP_AREA).apply(_parse_numeric)
    out["habitaciones"] = _first_existing(df_raw, COL_MAP_ROOMS).apply(_parse_int)
    out["banos"] = _first_existing(df_raw, COL_MAP_BATHS).apply(_parse_int)
    out["garajes"] = _first_existing(df_raw, COL_MAP_PARKING).apply(_parse_int)

    # Tipo inmueble
    out["tipo_inmueble"] = _first_existing(df_raw, COL_MAP_TYPE).apply(_normalize_tipo)

    # URL y fecha
    out["url"] = _first_existing(df_raw, COL_MAP_URL)
    out["fecha_extraccion"] = _first_existing(df_raw, COL_MAP_DATE)

    return out


print("=" * 70)
print("NORMALIZANDO ESQUEMAS")
print("=" * 70)

norm_frames: dict[str, pd.DataFrame] = {}
for fuente, df_raw in raw_frames.items():
    df_norm = normalizar_local(fuente, df_raw)
    norm_frames[fuente] = df_norm

# ═══════════════════════════════════════════════════════════════
# 3. FILTROS DE CALIDAD (gates)
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("FILTROS DE CALIDAD (gate de precio + id no nulo)")
print("=" * 70)

quality_rows = []
clean_frames = []

for fuente, df in norm_frames.items():
    n_raw = len(df)
    n_id = df["id_original"].notna().sum()
    n_id_valid = ((df["id_original"] != "None") & (df["id_original"] != "nan") & (df["id_original"].str.len() > 0)).sum()
    n_precio = df["precio_num"].notna().sum()
    n_gate = ((df["precio_num"] > MIN_PRICE) & (df["precio_num"] < MAX_PRICE)).sum()

    mask_clean = (
        (df["id_original"].notna())
        & (df["id_original"] != "None")
        & (df["id_original"] != "nan")
        & (df["precio_num"].notna())
        & (df["precio_num"] > MIN_PRICE)
        & (df["precio_num"] < MAX_PRICE)
    )
    df_clean = df[mask_clean].copy()
    n_clean = len(df_clean)

    # Validaciones de rango — convertir a float para permitir NaN
    for c_num in ["area_m2", "habitaciones", "banos", "garajes"]:
        df_clean[c_num] = df_clean[c_num].astype("float64")
    df_clean.loc[:, "area_m2"] = df_clean["area_m2"].where(
        (df_clean["area_m2"] >= 10) & (df_clean["area_m2"] <= 2000)
    )
    df_clean.loc[:, "habitaciones"] = df_clean["habitaciones"].where(
        (df_clean["habitaciones"] >= 0) & (df_clean["habitaciones"] <= 20)
    )
    df_clean.loc[:, "banos"] = df_clean["banos"].where(
        (df_clean["banos"] >= 0) & (df_clean["banos"] <= 15)
    )

    pct_surv = round(n_clean / n_raw * 100, 1) if n_raw else 0
    pct_area = round(df_clean["area_m2"].notna().sum() / n_clean * 100, 1) if n_clean else 0
    pct_city = round((df_clean["city_token"] != "otra_ciudad").sum() / n_clean * 100, 1) if n_clean else 0
    pct_market = round((df_clean["market_token"] != "mercado_otro").sum() / n_clean * 100, 1) if n_clean else 0

    quality_rows.append({
        "fuente": fuente,
        "raw": n_raw,
        "id_valido": int(n_id_valid),
        "con_precio": int(n_precio),
        "en_gate": int(n_gate),
        "clean": n_clean,
        "surv%": pct_surv,
        "area%": pct_area,
        "city%": pct_city,
        "mkt%": pct_market,
    })

    status = "✅" if n_clean > 0 else "⚠️"
    print(f"  {status} {fuente:25s}  raw={n_raw:>6,}  id_ok={n_id_valid:>6,}  precio_ok={n_gate:>5,}  clean={n_clean:>5,}  surv={pct_surv}%")

    if n_clean > 0:
        clean_frames.append(df_clean)

# Unir todo
df_silver = pd.concat(clean_frames, ignore_index=True) if clean_frames else pd.DataFrame()
n_silver = len(df_silver)
print(f"\n  TOTAL SILVER (pre-dedup): {n_silver:,} registros")

# Dedup: último registro por (id_original, fuente)
if n_silver > 0:
    df_silver = df_silver.sort_values("fecha_extraccion", ascending=False, na_position="last")
    df_silver = df_silver.drop_duplicates(subset=["id_original", "fuente"], keep="first")
    n_deduped = len(df_silver)
    print(f"  TOTAL SILVER (post-dedup): {n_deduped:,} registros")
else:
    n_deduped = 0

# ═══════════════════════════════════════════════════════════════
# 4. REPORTE DE CALIDAD POR FUENTE
# ═══════════════════════════════════════════════════════════════
print("\n" + "=" * 70)
print("CALIDAD POR FUENTE")
print("=" * 70)
df_quality = pd.DataFrame(quality_rows)
print(df_quality.to_string(index=False))

# ═══════════════════════════════════════════════════════════════
# 5. DISTRIBUCIÓN GEOGRÁFICA
# ═══════════════════════════════════════════════════════════════
if n_deduped > 0:
    print("\n" + "=" * 70)
    print("DISTRIBUCIÓN GEOGRÁFICA")
    print("=" * 70)

    city_dist = (
        df_silver.groupby("city_token")
        .agg(n=("id_original", "count"), precio_med=("precio_num", "median"))
        .sort_values("n", ascending=False)
    )
    city_dist["precio_med"] = city_dist["precio_med"].apply(lambda x: f"${x:,.0f}")
    print("\n── Top 20 ciudades ──")
    print(city_dist.head(20).to_string())
    print(f"\nTotal ciudades únicas: {len(city_dist)}")

    otra = city_dist.loc["otra_ciudad", "n"] if "otra_ciudad" in city_dist.index else 0
    print(f'Registros en "otra_ciudad": {otra} ({otra / n_deduped * 100:.1f}%)')

    market_dist = (
        df_silver.groupby("market_token")
        .agg(n=("id_original", "count"), precio_med=("precio_num", "median"))
        .sort_values("n", ascending=False)
    )
    market_dist["precio_med"] = market_dist["precio_med"].apply(lambda x: f"${x:,.0f}")
    print("\n── Mercados ──")
    print(market_dist.to_string())

    # Ubicaciones raw que caen en "otra_ciudad" — para mejorar catálogo
    otra_mask = df_silver["city_token"] == "otra_ciudad"
    if otra_mask.sum() > 0:
        print("\n── Top 20 ubicaciones SIN ciudad reconocida (para ampliar catálogo) ──")
        top_otra = (
            df_silver[otra_mask]
            .groupby("ubicacion_raw")
            .size()
            .sort_values(ascending=False)
            .head(20)
        )
        for loc, cnt in top_otra.items():
            print(f"  {cnt:>4}  {loc}")

    # ═══════════════════════════════════════════════════════════════
    # 6. DISTRIBUCIÓN DE PRECIOS
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("DISTRIBUCIÓN DE PRECIOS")
    print("=" * 70)

    price_stats = (
        df_silver.groupby("fuente")["precio_num"]
        .describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])
    )
    price_cols = ["count", "mean", "5%", "25%", "50%", "75%", "95%", "min", "max"]
    for c in price_cols:
        if c in price_stats.columns:
            price_stats[c] = price_stats[c].apply(lambda x: f"${x:,.0f}" if pd.notna(x) else "")
    print(price_stats[price_cols].to_string())

    # ═══════════════════════════════════════════════════════════════
    # 7. TIPO DE INMUEBLE
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("TIPO DE INMUEBLE")
    print("=" * 70)
    tipo_dist = df_silver.groupby("tipo_inmueble").agg(
        n=("id_original", "count"),
        precio_med=("precio_num", "median"),
        area_med=("area_m2", "median"),
    ).sort_values("n", ascending=False)
    tipo_dist["precio_med"] = tipo_dist["precio_med"].apply(lambda x: f"${x:,.0f}")
    tipo_dist["area_med"] = tipo_dist["area_med"].apply(
        lambda x: f"{x:.0f} m²" if pd.notna(x) else "-"
    )
    print(tipo_dist.to_string())

    # ═══════════════════════════════════════════════════════════════
    # 8. COMPLETITUD DE COLUMNAS
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("COMPLETITUD DE COLUMNAS")
    print("=" * 70)
    cols_check = [
        "id_original", "titulo", "precio_num", "area_m2",
        "habitaciones", "banos", "garajes", "tipo_inmueble",
        "ubicacion_raw", "city_token", "url",
    ]
    for c in cols_check:
        if c in df_silver.columns:
            n_ok = df_silver[c].notna().sum()
            # Para strings, excluir vacíos
            if df_silver[c].dtype == object:
                n_ok = ((df_silver[c].notna()) & (df_silver[c].astype(str).str.len() > 0)).sum()
            pct = n_ok / n_deduped * 100
            bar = "█" * int(pct // 5) + "░" * (20 - int(pct // 5))
            print(f"  {c:20s}  {bar}  {pct:5.1f}%  ({n_ok:,}/{n_deduped:,})")

    # ═══════════════════════════════════════════════════════════════
    # 9. CROSS-TAB: FUENTE × CIUDAD (top 10 ciudades)
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("COBERTURA FUENTE × CIUDAD (top 10 ciudades)")
    print("=" * 70)
    top_cities = city_dist.head(10).index.tolist()
    ct = pd.crosstab(df_silver["fuente"], df_silver["city_token"])
    ct_sub = ct[[c for c in top_cities if c in ct.columns]]
    ct_sub["TOTAL"] = ct.sum(axis=1)
    print(ct_sub.to_string())

    # ═══════════════════════════════════════════════════════════════
    # 10. ALERTAS Y RECOMENDACIONES
    # ═══════════════════════════════════════════════════════════════
    print("\n" + "=" * 70)
    print("ALERTAS Y RECOMENDACIONES")
    print("=" * 70)

    # Fuentes con poca data
    for _, row in df_quality.iterrows():
        issues = []
        if row["clean"] == 0:
            issues.append("0 registros limpios")
        elif row["clean"] < 10:
            issues.append(f"solo {row['clean']} registros")
        if row["surv%"] < 30:
            issues.append(f"supervivencia baja ({row['surv%']}%)")
        if row["city%"] < 40 and row["clean"] > 0:
            issues.append(f"ciudad reconocida baja ({row['city%']}%)")
        if row["area%"] < 30 and row["clean"] > 0:
            issues.append(f"área incompleta ({row['area%']}%)")
        if issues:
            print(f"  ⚠️  {row['fuente']:25s}  → {', '.join(issues)}")

    # Ciudades que faltan en catálogo
    if otra > 10:
        print(f"\n  📍 '{otra}' registros sin ciudad reconocida.")
        print("     Revisar ubicaciones listadas arriba para ampliar geography_catalog.py")

    # Fuentes sin precio numérico nativo
    no_precio_num = [f for f in FUENTES if f in raw_frames and "precio_num" not in raw_frames[f].columns]
    if no_precio_num:
        print(f"\n  💰 Fuentes SIN precio_num nativo (dependen del parseo de string): {no_precio_num}")

    print("\n" + "=" * 70)
    print(f"RESUMEN: {n_deduped:,} registros listos para Silver/Gold")
    print(f"         {len(df_quality[df_quality['clean'] > 0])} de {len(FUENTES)} fuentes con datos")
    print("=" * 70)

else:
    print("\n⚠️ 0 registros pasaron el filtro de calidad. Revisar datos crudos arriba.")
