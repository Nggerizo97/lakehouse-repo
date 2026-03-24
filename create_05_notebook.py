import json
import os

with open("04_Model_Training.ipynb", "r", encoding="utf-8") as f:
    template = json.load(f)

# Keep the metadata
metadata = template.get("metadata", {})

cells = []

def add_cell(source_code):
    cells.append({
        "cell_type": "code",
        "execution_count": None,
        "metadata": {},
        "outputs": [],
        "source": [line + "\n" if not line.endswith("\n") else line for line in source_code.split("\n")]
    })

# Cell 1: Setup
add_cell('''# =============================================================
# 05_BATCH_INFERENCE.IPYNB
# =============================================================
# Modern Data Stack: Inferencia Batch para pre-calcular la 
# rentabilidad_potencial de toda la tabla Gold (app_inmuebles)
# evitando colapsos de memoria en el frontend (Streamlit).

%run ./01_Init_Config''')

# Cell 2: Imports & Load Gold
add_cell('''import pandas as pd
import numpy as np
import pickle
import os
import sys

# Agregar src al path
sys.path.append(os.getcwd())

from pyspark.sql import functions as F
from src.ml.scorer import score_dataframe

GOLD_PATH = f"s3a://{BUCKET}/gold/app_inmuebles/"
SCORED_PATH = f"s3a://{BUCKET}/gold/app_inmuebles_scored/"
MODEL_DIR = "/dbfs/mnt/models/champion"  # Ajustar a tu ruta real de modelos

print(f"📖 Cargando capa Gold desde: {GOLD_PATH}")
df_gold_spark = spark.read.format("delta").load(GOLD_PATH)

# Convertir a Pandas para el scorer (30,000 filas caben perfecto en RAM del driver)
# En el futuro, si hay 1M+ filas, usar Pandas UDFs de PySpark
print("⬇️ Descargando Spark DataFrame a Pandas...")
df_pandas = df_gold_spark.toPandas()
print(f"✅ {len(df_pandas)} registros cargados.")''')

# Cell 3: Load Model Bundle
add_cell('''import glob

print(f"🔍 Buscando el último bundle de modelo en {MODEL_DIR}...")
try:
    pickles = glob.glob(os.path.join(MODEL_DIR, "bundle_v*.pkl"))
    if not pickles:
        raise FileNotFoundError(f"No se encontró ningún archivo .pkl en {MODEL_DIR}")
    
    # Tomar el más reciente
    latest_bundle_path = max(pickles, key=os.path.getmtime)
    print(f"📦 Cargando modelo Champion: {os.path.basename(latest_bundle_path)}")
    
    with open(latest_bundle_path, "rb") as f:
        bundle = pickle.load(f)
        
    print(f"✅ Modelo {bundle.get('model_version', 'v?')} cargado exitosamente.")
except Exception as e:
    print(f"❌ Error crítico cargando el modelo: {e}")
    raise e''')

# Cell 4: Apply Scorer
add_cell('''# Aplicar la misma lógica idéntica de Streamlit (scorer.py)
print("⚙️ Iniciando inferencia masiva y cálculo de rentabilidad...")
df_scored_pandas = score_dataframe(df_pandas, bundle)

print("✅ Inferencia completada.")
display(df_scored_pandas[["titulo", "city_token", "precio_num", "precio_predicho", "rentabilidad_potencial", "estado_inversion"]].head(10))''')

# Cell 5: Save Scored Table to S3
add_cell('''# Devolver a Spark y guardar como Delta en S3
print("⬆️ Subiendo DataFrame nuevamente a Spark...")

# Evitar problemas con tipos incompatibles en Spark
for col in df_scored_pandas.columns:
    if df_scored_pandas[col].dtype == "object":
        df_scored_pandas[col] = df_scored_pandas[col].fillna("").astype(str)

df_scored_spark = spark.createDataFrame(df_scored_pandas)

print(f"💾 Guardando tabla costeada en Inbound/Gold Scored: {SCORED_PATH}")
(
    df_scored_spark.write
    .format("delta")
    .mode("overwrite")
    .save(SCORED_PATH)
)

print("🎉 Misión Inferencia Batch (Modern Data Stack) Finalizada Exitosamente.")''')

notebook = {
    "cells": cells,
    "metadata": metadata,
    "nbformat": 4,
    "nbformat_minor": 4
}

with open("05_Batch_Inference.ipynb", "w", encoding="utf-8") as f:
    json.dump(notebook, f, indent=1, ensure_ascii=False)

print("Notebook 05 creado correctamente.")
