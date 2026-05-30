import boto3
import json
import os
import zipfile
import shutil
from datetime import datetime

# =============================================================
# 1. CONFIGURACIÓN — edita aquí qué quieres descargar
# =============================================================
BUCKET = "bronce-scrap-date"
REGION = "us-east-1"

# Tablas a descargar. Cada entrada es un prefijo S3.
# Cambia a False las que NO necesites en este run.
TARGETS = {
    # -- Gold (datos listos para análisis) --
    "gold/app_inmuebles_scored":  True,   # inmuebles con predicciones del modelo
    "gold/app_inmuebles":         False,  # inmuebles sin score (usa scored si existe)
    "gold/mercado_analitica":     True,   # KPIs de mercado por ciudad/zona
    "gold/mercado_sectorial":     True,   # KPIs por sector/comuna
    "gold/portal_operacion":      True,   # salud operativa de cada portal
    "gold/price_intelligence":    False,  # comparación cross-portal (grande)
    # -- Modelos --
    "models":                     True,   # bundles XGBoost (.pkl)
    # -- Silver (datos intermedios, más pesados) --
    "silver/master_deduped":      False,
    "silver/master_inmuebles":    False,
    # -- Bronze / Raw (datos crudos de scraping, muy pesados) --
    "bronze":                     False,
    "raw":                        False,
}

# Segmentos de ruta que siempre se omiten (archivos de sistema Delta/Spark)
SKIP_SEGMENTS = {
    "_delta_log",     # logs de transacciones Delta (miles de JSON inútiles)
    "_temporary",     # escrituras en curso de Spark
    "_checkpoints",   # checkpoints de streaming
    "__pycache__",
    ".ipynb_checkpoints",
}

# Extensiones que siempre se omiten
SKIP_EXTENSIONS = {".crc", ".tmp", ".lock"}

# =============================================================
# 2. CREDENCIALES
# =============================================================
try:
    with open("aws_secrets.json", "r") as f:
        config = json.load(f)
    print("Credenciales cargadas desde aws_secrets.json.")
except FileNotFoundError:
    raise SystemExit("Error: No se encontro 'aws_secrets.json' en la carpeta actual.")

s3_client = boto3.client(
    "s3",
    aws_access_key_id=config["aws_access_key"],
    aws_secret_access_key=config["aws_secret_key"],
    region_name=REGION,
)

# =============================================================
# 3. UTILIDADES
# =============================================================
def _debe_omitir(key: str) -> bool:
    """Devuelve True si el archivo debe saltarse."""
    partes = key.replace("\\", "/").split("/")
    if any(seg in SKIP_SEGMENTS for seg in partes):
        return True
    _, ext = os.path.splitext(key)
    if ext.lower() in SKIP_EXTENSIONS:
        return True
    return False


def _listar_prefijo(prefijo: str) -> list[dict]:
    """Lista todos los objetos bajo un prefijo S3, devuelve lista de dicts {key, size}."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objetos = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefijo):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith("/"):          # entrada de "carpeta" virtual
                continue
            if _debe_omitir(key):
                continue
            objetos.append({"key": key, "size": obj["Size"]})
    return objetos


def _nombre_tabla(prefijo: str) -> str:
    """Extrae el nombre de tabla del prefijo (última parte del path)."""
    return prefijo.rstrip("/").split("/")[-1]

# =============================================================
# 4. DESCARGA PRINCIPAL
# =============================================================
def ejecutar_descarga(output_dir: str | None = None, crear_zip: bool = True):
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    base_dir = output_dir or os.path.join(os.getcwd(), f"descarga_{ts}")
    os.makedirs(base_dir, exist_ok=True)

    prefijos_activos = [p for p, activo in TARGETS.items() if activo]
    if not prefijos_activos:
        raise SystemExit("No hay tablas activas en TARGETS. Habilita al menos una.")

    # --- Auditoría previa: qué hay y cuánto pesa ---
    print(f"\n{'='*60}")
    print(f"  BUCKET : {BUCKET}")
    print(f"  DESTINO: {base_dir}")
    print(f"{'='*60}")
    plan = {}
    total_bytes = 0
    for prefijo in prefijos_activos:
        objetos = _listar_prefijo(prefijo)
        n = len(objetos)
        mb = sum(o["size"] for o in objetos) / 1_048_576
        plan[prefijo] = objetos
        total_bytes += sum(o["size"] for o in objetos)
        estado = f"{n:>5} archivos  {mb:>8.1f} MB" if n else "  (vacio o no existe)"
        print(f"  {prefijo:<40} {estado}")

    print(f"\n  TOTAL estimado: {total_bytes/1_048_576:.1f} MB en {sum(len(v) for v in plan.values())} archivos")
    print(f"{'='*60}\n")

    respuesta = input("Confirmar descarga? [s/N]: ").strip().lower()
    if respuesta not in ("s", "si", "sí", "y", "yes"):
        print("Descarga cancelada.")
        return

    # --- Descarga organizada por tabla ---
    descargados = 0
    errores = 0
    for prefijo, objetos in plan.items():
        if not objetos:
            continue
        nombre_tabla = _nombre_tabla(prefijo)
        carpeta_tabla = os.path.join(base_dir, nombre_tabla)
        os.makedirs(carpeta_tabla, exist_ok=True)

        print(f"\n  Descargando {nombre_tabla} ({len(objetos)} archivos)...")
        for obj in objetos:
            key = obj["key"]
            # Aplanar: quitar el prefijo S3 y guardar solo el nombre del archivo
            # (o sub-ruta relativa al prefijo si hay particiones tipo part-0000.parquet)
            rel_path = key[len(prefijo):].lstrip("/")
            local_path = os.path.join(carpeta_tabla, rel_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            try:
                s3_client.download_file(BUCKET, key, local_path)
                descargados += 1
                print(f"    OK  {rel_path}")
            except Exception as exc:
                errores += 1
                print(f"    ERR {key}: {exc}")

    print(f"\n  Descarga completa: {descargados} OK, {errores} errores.")

    # --- ZIP opcional ---
    if crear_zip:
        zip_path = f"{base_dir}.zip"
        print(f"  Comprimiendo en {zip_path} ...")
        with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
            for root, _, files in os.walk(base_dir):
                for fname in files:
                    fpath = os.path.join(root, fname)
                    zf.write(fpath, os.path.relpath(fpath, base_dir))
        shutil.rmtree(base_dir)
        print(f"  ZIP listo: {zip_path}")
    else:
        print(f"  Archivos en: {base_dir}")

# =============================================================
# 5. EJECUCIÓN
# =============================================================
if __name__ == "__main__":
    # crear_zip=False deja la carpeta descomprimida (más fácil para inspeccionar)
    ejecutar_descarga(crear_zip=False)
