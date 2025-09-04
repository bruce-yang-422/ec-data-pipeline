import os
import pandas as pd
import json
import glob
import re
import difflib
import unicodedata
from typing import List, Dict, Any

RAW_DIR = r'D:\Projects\python_dev\ec-data-pipeline\data_raw\pchome'
OUTPUT_DIR = r'D:\Projects\python_dev\ec-data-pipeline\temp\pchome'
MAPPING_PATH = r'D:\Projects\python_dev\ec-data-pipeline\config\pchome_fields_mapping.json'

os.makedirs(OUTPUT_DIR, exist_ok=True)

# 讀入 mapping
with open(MAPPING_PATH, 'r', encoding='utf-8') as f:
    mapping: Dict[str, Dict[str, Any]] = json.load(f)

output_cols = [k for k in sorted(mapping.keys(), key=lambda k: int(mapping[k]['order']))]
mapping_zh = [mapping[k]['zh_name'] for k in mapping]
zh2en = {mapping[k]['zh_name']: k for k in mapping}

def clean_eq_quote(val: Any) -> str:
    if pd.isna(val):
        return ''
    val = str(val)
    if val.startswith('="') and val.endswith('"'):
        return val[2:-1]
    elif val.startswith('"') and val.endswith('"'):
        return val[1:-1]
    return val.strip()

def normalize_colname(s: str) -> str:
    s = str(s)
    s = re.sub(r'[（(【\[].*?[)）】\]]', '', s)  # 去除所有括號及內容
    s = unicodedata.normalize('NFKC', s)
    s = re.sub(r'[\s\-_/]', '', s).lower()
    return s

def smart_column_map(src_columns: List[str], mapping_zh_names: List[str]) -> Dict[str, str]:
    res: Dict[str, str] = {}
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
            res[std] = best if best is not None else ''
        else:
            res[std] = ''
    return res

def has_keyword(cols: List[str], keyword: str) -> bool:
    return any(keyword in c for c in cols)

def process_file(filepath: str) -> None:
    print(f'處理檔案: {filepath}')
    encodings_to_try = ['utf-8-sig', 'utf-8', 'utf-16', 'big5', 'cp950']
    read_success = False
    df = None
    columns = None
    first_line = ''
    for enc in encodings_to_try:
        try:
            # 先讀第一行取得欄位名稱
            with open(filepath, 'r', encoding=enc) as f:
                first_line = f.readline().strip()
            # 檢查第一行是否為警語
            if '警語' in first_line or '警告' in first_line:
                print(f'[SKIP] 檔案第一行為警語，跳過: {filepath}')
                return
            columns = first_line.split('\t')
            # 再用 names=columns 跳過第一行
            df = pd.read_csv(filepath, encoding=enc, sep='\t', header=None, names=columns, skiprows=1, dtype=str, keep_default_na=False)
            print(f"[INFO] 以編碼 {enc} 讀取成功，欄位數: {len(df.columns)}")
            read_success = True
            break
        except Exception as e:
            print(f"[WARN] 以編碼 {enc} 讀取失敗: {e}")
    if not read_success or df is None or columns is None:
        print(f"[ERROR] 所有常見編碼皆無法讀取 {filepath}")
        return

    src_cols = df.columns.tolist()
    norm_cols = [normalize_colname(c) for c in src_cols]
    # 模糊關鍵字比對
    if not (has_keyword(norm_cols, '退貨申請') and has_keyword(norm_cols, '審核通過')):
        print(f'[SKIP] 非退貨訂單資料檔案: {filepath}')
        return

    smart_map = smart_column_map(src_cols, mapping_zh)
    print("智慧自動對應表：", smart_map)

    # 建立來源欄位→標準英文欄位對應表，只保留有對應的欄位
    col_en_map: dict[str, str] = {}
    for en in mapping:
        zh = mapping[en]['zh_name']
        src_col = smart_map.get(zh, '')
        if src_col and src_col in df.columns:
            col_en_map[src_col] = en

    if not col_en_map:
        print(f'[SKIP] 無對應欄位: {filepath}')
        return

    df = df[[c for c in col_en_map.keys()]]
    df = df.rename(columns=col_en_map)

    for col in df.columns:
        df[col] = df[col].apply(clean_eq_quote)

    # === 自動生成與轉換欄位 ===
    # 1. 平台名稱
    df['platform'] = 'pchome'

    # 2. 從訂單編號提取訂單日期
    if 'order_sn' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_sn'].str[:8], format='%Y%m%d', errors='coerce')

    # 3. 計算訂單星期幾和第幾週（星期日=1, 星期一=2, ..., 星期六=7）
    if 'order_date' in df.columns:
        # pandas weekday: 0=週一, 6=週日
        df['order_weekday'] = df['order_date'].dt.weekday.apply(lambda x: 1 if x == 6 else x + 2)
        df['order_week'] = df['order_date'].dt.isocalendar().week

    # 4. 從商品名稱提取商品ID和選項編號（參考 mapping 與 cleaner 實作）
    def extract_product_id_option(name):
        if not isinstance(name, str): return pd.Series([None, None])
        m = re.search(r'\(([\w\-]+)-(\d{1,3})\)$', name)
        if m:
            option = str(m.group(2)).zfill(3)
            return pd.Series([m.group(1), option])
        else:
            return pd.Series([None, None])
    if 'product_name' in df.columns:
        df[['product_id', 'sku_option']] = df['product_name'].apply(extract_product_id_option)

    # 5. 明細序號 item_seq
    if 'item_seq' not in df.columns:
        if 'NO' in df.columns:
            df['item_seq'] = df['NO']
        else:
            # 若無 NO 欄位，對 order_sn 分組自動流水號
            if 'order_sn' in df.columns:
                df['item_seq'] = df.groupby('order_sn').cumcount() + 1
            else:
                df['item_seq'] = 1

    # 6. 合成唯一訂單識別碼
    if 'order_sn' in df.columns and 'item_seq' in df.columns:
        df['order_id'] = df['order_sn'].astype(str) + df['item_seq'].astype(str)

    # 7. confirm 欄位：「已確認」→ True，其他 → False
    if 'confirm' in df.columns:
        df['confirm'] = df['confirm'].apply(lambda x: True if str(x).strip() == '已確認' else False)

    # 8. 移除重複記錄（以 order_id 為基準）
    if 'order_id' in df.columns:
        df = df.drop_duplicates(subset=['order_id'])

    # 9. 數值欄位轉型
    numeric_cols = [
        'order_qty', 'quantity', 'cancel_qty', 'price_unit', 'price_total',
        'weight_total_kg', 'weight_max_kg', 'product_weight_kg',
        'package_len', 'package_wid', 'package_hei'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 10. 日期欄位轉型
    date_cols = [
        'order_date', 'ship_date', 'transfer_date', 'preorder_date',
        'return_apply_date', 'return_approve_date'
    ]
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # receiver_zip 只留前三碼
    if 'receiver_zip' in df.columns:
        df['receiver_zip'] = df['receiver_zip'].apply(lambda x: str(x).strip()[:3] if pd.notnull(x) else x)

    # receiver_addr 若 mapping 對不到，嘗試用關鍵字自動對應（補在 output_cols_present 前）
    if ('receiver_addr' not in df.columns or df['receiver_addr'].isnull().all() or (df['receiver_addr'] == '').all()):
        addr_col = [c for c in df.columns if ('地址' in c) or ('address' in c.lower())]
        if addr_col:
            print(f"[DEBUG] 自動對應 receiver_addr 用欄位: {addr_col[0]}")
            df['receiver_addr'] = df[addr_col[0]]
        else:
            print("[DEBUG] 找不到含有地址的欄位，現有欄位：", df.columns.tolist())

    # 欄位順序統一，未對應欄位自動跳過
    output_cols_present = [col for col in output_cols if col in df.columns]
    df = df[output_cols_present]

    # 依 轉單日期 或 return_apply_date 命名輸出檔案
    date_col = None
    if '轉單日期' in df.columns:
        date_col = '轉單日期'
    elif 'return_apply_date' in df.columns:
        date_col = 'return_apply_date'
    if date_col:
        order_dates = pd.to_datetime(df[date_col], errors='coerce')
        if order_dates.notnull().any():
            min_date = order_dates.min()
            max_date = order_dates.max()
            min_str = min_date.strftime('%Y%m%d') if pd.notnull(min_date) else 'NA'
            max_str = max_date.strftime('%Y%m%d') if pd.notnull(max_date) else 'NA'
            out_name = f"pchome_return_{min_str}-{max_str}.csv"
        else:
            fname = os.path.splitext(os.path.basename(filepath))[0]
            out_name = f'{fname}_return_cleaned.csv'
    else:
        fname = os.path.splitext(os.path.basename(filepath))[0]
        out_name = f'{fname}_return_cleaned.csv'
    outpath = os.path.join(OUTPUT_DIR, out_name)
    df.to_csv(outpath, index=False, encoding='utf-8-sig')
    print(f'[OK] 已輸出退貨檔案: {outpath}')

def batch_clean() -> None:
    files = glob.glob(os.path.join(RAW_DIR, '*'))
    for f in files:
        process_file(f)

if __name__ == '__main__':
    batch_clean()
