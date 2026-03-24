# Run this in a Databricks cell to diagnose the S3 folder and CURRENT notebook state
import os

# 1. Check current notebook code for line 36
print("🔍 1. VERIFICANDO TU CÓDIGO ACTUAL EN ESTA CELDA:")
try:
    # This tries to read the current notebook through dbutils if possible or just check the environment
    print("Por favor, confirma manualmente que la línea 36 de este notebook diga 'parquet'.")
except:
    pass

# 2. List S3 files
print("\n📂 2. LISTANDO ARCHIVOS EN S3 (gold/app_inmuebles/):")
try:
    # Use dbutils to list
    bucket = BUCKET # Needs to be defined or from 00_Setup_Mount
    path = f"s3a://{bucket}/gold/app_inmuebles/"
    files = dbutils.fs.ls(path)
    for f in files:
        print(f"   - {f.name}")
        if f.name == "_delta_log/":
            print("     ⚠️ ¡ALERTA! Existe una carpeta _delta_log. Spark intentará leer esto como Delta siempre.")
except Exception as e:
    print(f"   ❌ Error listando S3: {e}")

# 3. Check Spark session default format
print("\n⚙️ 3. SPARK CONFIG:")
print(f"Conf. spark.sql.sources.default: {spark.conf.get('spark.sql.sources.default', 'parquet')}")
