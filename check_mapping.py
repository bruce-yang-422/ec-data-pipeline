import pandas as pd
import json

# 讀取 CSV 檔案
df = pd.read_csv('temp/shopee/A01_master_orders_cleaned_for_bigquery.csv', nrows=1)
csv_cols = set(df.columns)

# 讀取 mapping 檔案
with open('config/shopee_fields_mapping.json', 'r', encoding='utf-8') as f:
    mapping = json.load(f)
mapping_cols = set(mapping.keys())

# 比較欄位
missing_in_mapping = csv_cols - mapping_cols
extra_in_mapping = mapping_cols - csv_cols

print('CSV欄位總數:', len(csv_cols))
print('Mapping欄位總數:', len(mapping_cols))
print('CSV有但Mapping沒有的欄位:', sorted(missing_in_mapping))
print('Mapping有但CSV沒有的欄位:', sorted(extra_in_mapping))

# 檢查是否有重複的 zh_name
zh_names = [mapping[col]['zh_name'] for col in mapping.keys()]
duplicates = [name for name in set(zh_names) if zh_names.count(name) > 1]
if duplicates:
    print('重複的 zh_name:', duplicates)
else:
    print('沒有重複的 zh_name')

# 顯示多出來的欄位詳細資訊
if extra_in_mapping:
    print('\n多出來的欄位詳細資訊:')
    for col in sorted(extra_in_mapping):
        print(f'  {col}: {mapping[col]["zh_name"]} (order: {mapping[col]["order"]})') 