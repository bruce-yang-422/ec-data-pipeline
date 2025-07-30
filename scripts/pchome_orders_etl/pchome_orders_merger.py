import os
import pandas as pd
import glob
import json

CLEANED_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
MERGED_DIR = r'D:\Projects\python_dev\ec-data-pipeline\data_processed\merged'
MAPPING_PATH = r'D:\Projects\python_dev\ec-data-pipeline\config\pchome_fields_mapping.json'

# 讀入 mapping 欄位順序
with open(MAPPING_PATH, 'r', encoding='utf-8') as f:
    mapping = json.load(f)
output_cols = [k for k, v in sorted(mapping.items(), key=lambda x: int(x[1]['order']))]

os.makedirs(MERGED_DIR, exist_ok=True)

# 唯一鍵（BigQuery/Postgres 都推薦 composite key）
UNIQUE_KEYS = ['platform', 'order_id']

def merge_cleaned_files():
    # 同時納入 pchome_*.csv 及 pchome_return_*.csv
    files = []
    files += glob.glob(os.path.join(CLEANED_DIR, 'pchome_*.csv'))
    files += glob.glob(os.path.join(CLEANED_DIR, 'pchome_return_*.csv'))
    print(f"[INFO] 找到 {len(files)} 個清洗後檔案")
    if not files:
        print("[WARN] 無清洗後檔案")
        return
    all_df = []
    for f in files:
        df = pd.read_csv(f, dtype=str, keep_default_na=False)
        all_df.append(df)
        print(f" 讀入 {f}, 筆數={len(df)}")
    merged = pd.concat(all_df, ignore_index=True)
    print(f"[INFO] 合併總筆數：{len(merged)}")

    # 欄位補齊
    for col in output_cols:
        if col not in merged:
            merged[col] = ''
    merged = merged[output_cols]

    # 去重覆（新蓋舊，保留最後一筆）
    merged = merged.drop_duplicates(subset=UNIQUE_KEYS, keep='last').reset_index(drop=True)
    print(f"[INFO] 合併後唯一鍵去重：{len(merged)}")

    outpath = os.path.join(MERGED_DIR, 'pchome_orders_merged.csv')
    merged.to_csv(outpath, index=False, encoding='utf-8-sig')
    print(f"[INFO] 已輸出合併檔案：{outpath}")

if __name__ == '__main__':
    merge_cleaned_files()
