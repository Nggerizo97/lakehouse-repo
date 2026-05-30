"""
Diagnóstico rápido de Bronze en S3.
Muestra estructura, columnas, tipos y muestra de datos por fuente.
"""
import json
import boto3

with open("aws_secrets.json", "r") as f:
    config = json.load(f)

BUCKET = config.get("bucket_name", "bronce-scrap-date")
s3 = boto3.client(
    "s3",
    aws_access_key_id=config["aws_access_key"],
    aws_secret_access_key=config["aws_secret_key"],
)

# ── 1. Listar carpetas top-level ──
print(f"{'='*60}")
print(f"BUCKET: {BUCKET}")
print(f"{'='*60}")

resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="", Delimiter="/")
top_folders = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
print(f"\nCarpetas top-level: {top_folders}")

# ── 2. Listar fuentes en bronze/ ──
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="bronze/", Delimiter="/")
fuentes = [
    p["Prefix"].replace("bronze/", "").strip("/")
    for p in resp.get("CommonPrefixes", [])
]
print(f"\nFuentes en bronze/: {fuentes}")

# ── 3. Para cada fuente, mostrar archivos y tamaño ──
for fuente in fuentes:
    prefix = f"bronze/{fuente}/"
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=20)
    objects = resp.get("Contents", [])
    total_size = sum(o["Size"] for o in objects)
    print(f"\n{'─'*50}")
    print(f"📁 bronze/{fuente}/  ({len(objects)} objetos, {total_size/1024:.1f} KB)")
    for obj in objects[:10]:
        key = obj["Key"].replace(prefix, "")
        print(f"   {key}  ({obj['Size']/1024:.1f} KB)")
    if len(objects) > 10:
        print(f"   ... y {len(objects)-10} más")

# ── 4. Revisar silver/ y gold/ ──
for layer in ["silver/", "gold/"]:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=layer, Delimiter="/")
    sub = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    print(f"\n{'─'*50}")
    print(f"📁 {layer}  subcarpetas: {sub}")

# ── 5. Leer muestra de Parquet/Delta con pandas si es posible ──
try:
    import pyarrow.parquet as pq
    import s3fs

    fs = s3fs.S3FileSystem(
        key=config["aws_access_key"],
        secret=config["aws_secret_key"],
    )

    print(f"\n{'='*60}")
    print("MUESTRA DE DATOS POR FUENTE (primeras 5 filas)")
    print(f"{'='*60}")

    for fuente in fuentes:
        prefix = f"{BUCKET}/bronze/{fuente}/"
        try:
            parquet_files = [
                f for f in fs.ls(prefix, detail=False)
                if f.endswith(".parquet")
            ]
            if not parquet_files:
                # Delta table — buscar en subcarpetas
                all_files = fs.glob(f"{prefix}**/*.parquet")
                parquet_files = all_files[:1]

            if parquet_files:
                df = pq.read_table(parquet_files[0], filesystem=fs).to_pandas()
                print(f"\n{'─'*50}")
                print(f"📊 {fuente}: {len(df)} filas, {len(df.columns)} columnas")
                print(f"   Columnas: {list(df.columns)}")
                print(f"   Dtypes:")
                for col_name, dtype in df.dtypes.items():
                    print(f"      {col_name}: {dtype}")
                print(f"\n   Muestra (5 filas):")
                print(df.head().to_string(max_colwidth=60))
            else:
                print(f"\n⚠️ {fuente}: sin archivos .parquet encontrados")
        except Exception as exc:
            print(f"\n⚠️ {fuente}: error leyendo parquet — {exc}")

except ImportError:
    print("\n⚠️ pyarrow/s3fs no instalados. Instala con: pip install pyarrow s3fs")
    print("   Solo se mostró la estructura de archivos.")
