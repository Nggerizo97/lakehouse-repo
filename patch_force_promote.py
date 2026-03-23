import json

try:
    with open("04_Model_Training.ipynb", "r", encoding="utf-8") as f:
        nb = json.load(f)

    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            src = "".join(cell["source"])
            if "FORCE_PROMOTE_TO_CHAMPION" in src:
                new_src = src.replace("FORCE_PROMOTE_TO_CHAMPION     = False", "FORCE_PROMOTE_TO_CHAMPION     = True")
                new_src = new_src.replace("FORCE_PROMOTE_TO_CHAMPION = False", "FORCE_PROMOTE_TO_CHAMPION = True")
                
                if new_src != src:
                    lines = new_src.split("\n")
                    new_source = [l + "\n" for l in lines[:-1]] + [lines[-1]] if lines else []
                    cell["source"] = new_source
                    print("Patched FORCE_PROMOTE_TO_CHAMPION.")
                    
    with open("04_Model_Training.ipynb", "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print("Notebook successfully set to force promote!")
    
except Exception as e:
    print("Error:", e)
