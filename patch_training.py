import json

try:
    with open("04_Model_Training.ipynb", "r", encoding="utf-8") as f:
        nb = json.load(f)

    for cell in nb["cells"]:
        if cell["cell_type"] == "code":
            src = "".join(cell["source"])
            if "XGBRegressor" in src:
                # Replace min_child_weight=5 with min_child_weight=15
                new_src = src.replace("min_child_weight=5", "min_child_weight=15")
                # Also slightly reduce estimators to prevent overfitting
                new_src = new_src.replace("n_estimators=900", "n_estimators=750") 
                
                # Reconstruct Jupyter list of lines
                if new_src != src:
                    lines = new_src.split("\n")
                    new_source = [l + "\n" for l in lines[:-1]] + [lines[-1]] if lines else []
                    cell["source"] = new_source
                    print("Patched XGBRegressor hyperparameters.")
                    
    with open("04_Model_Training.ipynb", "w", encoding="utf-8") as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print("Notebook perfectly patched!")
    
except Exception as e:
    print("Error:", e)
