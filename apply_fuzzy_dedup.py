import json
import os

NOTEBOOK_PATH = "03_Gold_Layer.ipynb"

with open(NOTEBOOK_PATH, "r", encoding="utf-8") as f:
    nb = json.load(f)

# El código completo que queremos inyectar
NEW_DEDUP_CODE = """# ── Dedup intra-portal (Fuzzy): mantener solo el perfil físico más reciente ──
# Agrupamos por lo que define al inmueble, ignorando el ID original que suele cambiar.
_fuzzy_keys = ["fuente", "city_token", "tipo_inmueble", "precio_num", "area_m2", "habitaciones", "banos"]
_w_latest = Window.partitionBy(*_fuzzy_keys).orderBy(F.desc("fecha_extraccion"))

_n_before_dedup_fuzzy = df_silver.count()
df_silver = (
    df_silver
    .withColumn("_dedup_rank", F.row_number().over(_w_latest))
    .filter(F.col("_dedup_rank") == 1)
    .drop("_dedup_rank")
)
_n_after_dedup_fuzzy = df_silver.count()
print(f"🧹 Dedup Fuzzy: {_n_before_dedup_fuzzy:,} -> {_n_after_dedup_fuzzy:,} inmuebles únicos.")"""

modified = False
for cell in nb.get("cells", []):
    if cell.get("cell_type") == "code":
        source = cell.get("source", [])
        source_str = "".join(source)
        
        # Buscamos el inicio del bloque (nuevo o viejo)
        if "Window.partitionBy" in source_str and "df_silver" in source_str:
            print("✨ Encontrado celda de deduplicación. Limpiando y aplicando parche...")
            
            # Buscamos dónde empieza y dónde termina el bloque de dedup
            start_idx = -1
            end_idx = -1
            for i, line in enumerate(source):
                if "Window.partitionBy" in line:
                    # Retrocedemos para encontrar el comentario anterior
                    start_idx = i
                    if i > 0 and "# ── Dedup" in source[i-1]:
                        start_idx = i - 1
                    if start_idx > 0 and "# Sin esto" in source[start_idx-1]:
                        start_idx = start_idx - 2 # capturar todo el intro
                        
                if "print(f\"" in line and "Dedup" in line:
                    # El bloque termina después de los paréntesis de cierre del print
                    end_idx = i + 1
                    # Si la siguiente línea es una continuación del f-string (empieza con f")
                    while end_idx < len(source) and (source[end_idx].strip().startswith('f"') or source[end_idx].strip().startswith(')') or source[end_idx].strip() == ""):
                        if "# ── Leer" in source[end_idx]: break
                        end_idx += 1
                    break
            
            if start_idx != -1 and end_idx != -1:
                new_lines = [line + "\n" for line in NEW_DEDUP_CODE.split("\n")]
                cell["source"] = source[:start_idx] + new_lines + ["\n"] + source[end_idx:]
                modified = True
                break

if modified:
    with open(NOTEBOOK_PATH, "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print(f"✅ Notebook {NOTEBOOK_PATH} actualizado correctamente.")
else:
    print("⚠️ No se pudo localizar el bloque de código.")
