import json
import sys

try:
    with open('04_Model_Training.ipynb', 'r', encoding='utf-8') as f:
        nb = json.load(f)
    
    with open('training_cells_dump.txt', 'w', encoding='utf-8') as out:
        out.write(f'Total cells: {len(nb["cells"])}\n')
        for i, cell in enumerate(nb['cells']):
            if cell['cell_type'] == 'code':
                src = ''.join(cell['source'])
                if 'XGBRegressor' in src or 'train_test_split' in src or 'Pipeline' in src or 'outliers' in src or 'feature_cols' in src or 'comuna_mercado' in src:
                    out.write(f'\n================ CELL {i} ================\n')
                    out.write(src)
except Exception as e:
    print('Error:', e)
