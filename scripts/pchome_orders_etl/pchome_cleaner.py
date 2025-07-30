import os
import pandas as pd
import numpy as np
import json
import glob
import re
import difflib
import unicodedata

RAW_DIR = r'D:\Projects\python_dev\ec-data-pipeline\data_raw\pchome'
OUTPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
MAPPING_PATH = r'D:\Projects\python_dev\ec-data-pipeline\config\pchome_fields_mapping.json'

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'pchome_cleaner.log')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 讀入 mapping
with open(MAPPING_PATH, 'r', encoding='utf-8') as f:
    mapping = json.load(f)

output_cols = [k for k, v in sorted(mapping.items(), key=lambda x: int(x[1]['order']))]
mapping_zh = [v['zh_name'] for v in mapping.values()]
zh2en = {v['zh_name']: k for k, v in mapping.items()}

def log(msg):
    print(msg)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')

def clean_eq_quote(val):
    if pd.isnull(val):
        return np.nan
    val = str(val)
    if val.startswith('="') and val.endswith('"'):
        return val[2:-1]
    elif val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val.strip()

def extract_product_id_option(name):
    if not isinstance(name, str): return pd.Series([np.nan, np.nan])
    m = re.search(r'\(([\w\-]+)-(\d{1,3})\)$', name)
    if m:
        option = str(m.group(2)).zfill(3)
        return pd.Series([m.group(1), option])
    else:
        return pd.Series([np.nan, np.nan])

def normalize_colname(s):
    import unicodedata, re
    s = str(s)
    s = re.sub(r'[（(【\[].*?[)）】\]]', '', s)  # 去除所有括號及內容
    s = unicodedata.normalize('NFKC', s)
    s = re.sub(r'[\s\-_/]', '', s).lower()
    return s

def smart_column_map(src_columns, mapping_zh_names):
    res = {}
    src_map = {normalize_colname(c): c for c in src_columns}
    for std in mapping_zh_names:
        std_norm = normalize_colname(std)
        if std_norm in src_map:
            res[std] = src_map[std_norm]
            continue
        best = None
        best_score = 0
        for src_norm, src in src_map.items():
            score = difflib.SequenceMatcher(None, std_norm, src_norm).ratio()
            if score > best_score:
                best, best_score = src, score
        if best_score > 0.85:
            res[std] = best
        else:
            res[std] = None
    return res

def process_file(filepath):
    log(f'處理檔案: {filepath}')
    ext = os.path.splitext(filepath)[-1].lower()
    # 僅處理第一行為警語的檔案，欄位對應從第二行開始
    header_row = None
    first_line = ''
    used_encoding = None
    for enc in ['utf-16', 'utf-8']:
        try:
            with open(filepath, 'r', encoding=enc, errors='ignore') as f:
                first_line = f.readline().strip()
            used_encoding = enc
            break
        except Exception:
            continue
    if not first_line.startswith('【※此匯出資料'):
        log(f'[SKIP] 非一般訂單檔案（第一行不是警語）: {filepath}')
        return
    header_row = 1
    if ext in ['.csv', '.CSV']:
        try:
            df = pd.read_csv(filepath, encoding=used_encoding, sep='\t', header=header_row, dtype=str, keep_default_na=False)
        except Exception as e:
            log(f'[ERROR] 讀檔失敗: {e}')
            return
    elif ext in ['.xls', '.xlsx']:
        df = pd.read_excel(filepath, header=header_row, dtype=str)
    else:
        log(f'[WARN] 不支援的檔案格式: {filepath}')
        return

    log("原始欄位：" + str(df.columns.tolist()))

    # 辨識退貨資料，若同時有 return_apply_date 與 return_approve_date 欄位則跳過
    cols = [c.lower() for c in df.columns]
    if ('return_apply_date' in cols) and ('return_approve_date' in cols):
        log(f'[SKIP] 跳過退貨資料檔案: {filepath}')
        return

    src_cols = df.columns.tolist()
    smart_map = smart_column_map(src_cols, mapping_zh)
    log("智慧自動對應表：" + str(smart_map))

    # 建立來源欄位→標準英文欄位對應表，只對應存在於 df.columns 的欄位
    col_en_map = {}
    for en, v in mapping.items():
        zh = v['zh_name']
        src_col = smart_map.get(zh)
        if src_col and src_col in df.columns:
            col_en_map[src_col] = en
    if not col_en_map:
        log(f'[ERROR] 無法對應任何欄位，請檢查 mapping 與原始欄位名稱')
        return

    df = df[[c for c in col_en_map.keys()]]
    df = df.rename(columns=col_en_map)

    for col in df.columns:
        df[col] = df[col].apply(clean_eq_quote)

    # confirm 欄位：「已確認」= True，其餘 = False
    if 'confirm' in df.columns:
        df['confirm'] = df['confirm'].apply(lambda x: True if str(x).strip() == '已確認' else False)

    # is_merge_box 欄位：「是」= True，其餘 = False
    if 'is_merge_box' in df.columns:
        df['is_merge_box'] = df['is_merge_box'].apply(lambda x: True if str(x).strip() == '是' else False)

    # receiver_zip 只留前三碼
    if 'receiver_zip' in df.columns:
        df['receiver_zip'] = df['receiver_zip'].apply(lambda x: str(x).strip()[:3] if pd.notnull(x) else x)

    # receiver_addr 若 mapping 對不到，嘗試用關鍵字自動對應（補在 output_cols 補空欄前）
    if ('receiver_addr' not in df.columns or df['receiver_addr'].isnull().all() or (df['receiver_addr'] == '').all()):
        addr_col = [c for c in df.columns if ('地址' in c) or ('address' in c.lower())]
        if addr_col:
            log(f"[DEBUG] 自動對應 receiver_addr 用欄位: {addr_col[0]}")
            df['receiver_addr'] = df[addr_col[0]]
        else:
            log("[DEBUG] 找不到含有地址的欄位，現有欄位：" + str(df.columns.tolist()))

    # item_seq 如無自動補流水號，否則直接用來源NO
    if 'item_seq' not in df.columns:
        log("[INFO] 未偵測到明細序號，將自動補流水號 item_seq")
        df['item_seq'] = df.groupby('order_sn').cumcount() + 1
        df['item_seq'] = df['item_seq'].apply(lambda x: str(x).zfill(2))

    # 合成唯一鍵
    df['order_id'] = df['order_sn'] + '-' + df['item_seq']

    # 訂單日期、星期幾、第幾週
    df['order_date'] = pd.to_datetime(df['order_sn'].str[:8], format='%Y%m%d', errors='coerce')
    # 星期日=1，星期一=2，...，星期六=7
    df['order_weekday'] = df['order_date'].dt.weekday.apply(lambda x: 1 if x == 6 else x + 2)
    df['order_week'] = df['order_date'].dt.isocalendar().week

    # 商品ID與選項編號
    df[['product_id', 'sku_option']] = df['product_name'].apply(extract_product_id_option)

    # 數值欄型態轉換
    for col in [
        'order_qty', 'quantity', 'cancel_qty', 'price_unit', 'price_total',
        'product_weight_kg', 'weight_total_kg', 'weight_max_kg',
        'package_len', 'package_wid', 'package_hei'
    ]:
        if col in df:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 出貨日期正規化
    if 'ship_date' in df:
        df['ship_date'] = pd.to_datetime(df['ship_date'], errors='coerce')

    # 平台名稱
    df['platform'] = 'pchome'

    # 補空欄
    for col in output_cols:
        if col not in df:
            df[col] = np.nan

    df = df.drop_duplicates(subset=['order_id'])
    df = df[output_cols]

    # 依 order_date 命名輸出檔案
    if 'order_date' in df.columns:
        order_dates = pd.to_datetime(df['order_date'], errors='coerce')
        min_date = order_dates.min()
        max_date = order_dates.max()
        min_str = min_date.strftime('%Y%m%d') if pd.notnull(min_date) else 'NA'
        max_str = max_date.strftime('%Y%m%d') if pd.notnull(max_date) else 'NA'
        out_name = f"pchome_{min_str}_{max_str}.csv"
    else:
        out_name = os.path.splitext(os.path.basename(filepath))[0] + '_cleaned.csv'
    out_path = os.path.join(OUTPUT_DIR, out_name)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
    log(f'已輸出: {out_path}')

def batch_clean():
    files = []
    log(f"[INFO] RAW_DIR: {RAW_DIR}")
    for ext in ['csv', 'CSV', 'xls', 'xlsx']:
        files_found = glob.glob(os.path.join(RAW_DIR, f'*.{ext}'))
        if files_found:
            log(f"[INFO] 找到 {len(files_found)} 個 *.{ext} 檔案")
        files.extend(files_found)
    if not files:
        log(f"[INFO] 找不到任何支援的檔案於 {RAW_DIR}")
        return
    log("[INFO] 將處理以下檔案：")
    for f in files:
        log(f)
    for f in files:
        process_file(f)

if __name__ == '__main__':
    batch_clean()
