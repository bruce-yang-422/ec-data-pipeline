import os
import pandas as pd
import numpy as np
import json
import glob
import re
import unicodedata
from datetime import datetime

RAW_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'data_raw', 'Yahoo')
OUTPUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'temp', 'Yahoo')
MAPPING_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'config', 'yahoo_fields_mapping.json')
LOG_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), 'logs')
LOG_FILE = os.path.join(LOG_DIR, 'yahoo_cleaner.log')
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

# 讀入 mapping
with open(MAPPING_PATH, 'r', encoding='utf-8') as f:
    mapping = json.load(f)

output_cols = [k for k, v in sorted(mapping.items(), key=lambda x: int(x[1]['order']))]
mapping_zh = [v['zh_name'] for v in mapping.values()]
zh2en = {v['zh_name']: k for k, v in mapping.items()}


def log(msg: str):
    print(msg)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(str(msg) + '\n')

def clean_value(val: str) -> str | float:
    if pd.isnull(val):
        return np.nan
    val = str(val)
    val = unicodedata.normalize('NFKC', val)  # 全形轉半形
    val = re.sub(r'[\s\u3000]+', '', val)  # 去除所有空白
    val = re.sub(r'[\r\n\t]', '', val)  # 去除換行與 tab
    val = re.sub(r'[\uFFFD\u200B\uFEFF]', '', val)  # 去除特殊符號
    val = re.sub(r'[\u0000-\u001F\u007F-\u009F]', '', val)  # 控制字元
    val = re.sub(r'[\u202A-\u202E]', '', val)  # 方向控制符
    return val.strip()

def normalize_colname(s: str) -> str:
    s = str(s)
    s = re.sub(r'[（(【\[].*?[)）】\]]', '', s)  # 去除所有括號及內容
    s = unicodedata.normalize('NFKC', s)
    s = re.sub(r'[\s\-_/]', '', s).lower()
    return s

def smart_column_map(src_columns: list[str], mapping_zh_names: list[str]) -> dict[str, str | None]:
    import difflib
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
    file_ext = os.path.splitext(filepath)[1].lower()
    
    if file_ext in ['.xls', '.xlsx']:
        # 處理 Excel 檔案
        try:
            if file_ext == '.xls':
                df = pd.read_excel(filepath, dtype=str, engine='xlrd')
            else:  # .xlsx
                df = pd.read_excel(filepath, dtype=str, engine='openpyxl')
            df.columns = [col.strip() for col in df.columns]
            log(f"成功讀取 Excel 檔案：{filepath}")
        except Exception as e:
            log(f"[ERROR] 無法讀取 Excel 檔案：{filepath}, 錯誤：{e}")
            # 嘗試其他 engine
            try:
                if file_ext == '.xls':
                    df = pd.read_excel(filepath, dtype=str, engine='openpyxl')
                else:
                    df = pd.read_excel(filepath, dtype=str, engine='xlrd')
                df.columns = [col.strip() for col in df.columns]
                log(f"使用備用 engine 成功讀取 Excel 檔案：{filepath}")
            except Exception as e2:
                log(f"[ERROR] 備用 engine 也失敗：{filepath}, 錯誤：{e2}")
                return None
    else:
        # 處理 CSV 檔案
        encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(filepath, dtype=str, encoding=encoding)
                df.columns = [col.strip() for col in df.columns]
                log(f"成功使用編碼：{encoding}")
                break
            except Exception as e:
                log(f"[WARN] 編碼 {encoding} 失敗: {e}")
                continue
        if df is None:
            log(f"[ERROR] 無法讀取檔案：{filepath}")
            return None
    
    log(f"原始資料欄位: {list(df.columns)}")
    log(f"原始資料筆數: {len(df)}")
    df = df.fillna('')
    # 欄位自動對應
    col_map = smart_column_map(df.columns, mapping_zh)
    log(f"欄位對應結果: {col_map}")
    # 轉成標準英文欄位
    std_df = pd.DataFrame()
    for zh, en in zh2en.items():
        src_col = col_map.get(zh)
        if src_col and src_col in df.columns:
            std_df[en] = df[src_col].apply(clean_value)
        else:
            std_df[en] = np.nan
    # 固定欄位
    std_df['platform'] = 'yahoo'
    # order_date: 由 order_sn 前6碼解析 (YYMMDD)
    if 'order_sn' in std_df.columns:
        std_df['order_date'] = std_df['order_sn'].apply(lambda x: pd.to_datetime('20'+x[2:8], format='%Y%m%d', errors='coerce') if isinstance(x, str) and len(x) >= 8 else pd.NaT)
        log(f"order_date 由 order_sn 產生，範例: {std_df[['order_sn','order_date']].head().to_dict()}")
    # line_number: 針對相同 recipient_name 及 recipient_address，依 order_sn 排序給流水號
    group_cols = []
    for c in ['recipient_name', 'recipient_address']:
        if c in std_df.columns:
            group_cols.append(c)
    if group_cols and 'order_sn' in std_df.columns:
        std_df = std_df.sort_values(group_cols + ['order_sn']).reset_index(drop=True)
        std_df['line_number'] = std_df.groupby(group_cols).cumcount() + 1
        log(f"line_number 依 {group_cols} + order_sn排序產生，前幾筆: {std_df[group_cols+['order_sn','line_number']].head().to_dict()}")
    else:
        std_df['line_number'] = 1
        log("[WARN] 缺少分組欄位或 order_sn，line_number 全部設為 1")
    std_df['line_number'] = std_df['line_number'].fillna(1).astype(int)
    # 依 mapping 補空欄
    for col in output_cols:
        if col not in std_df:
            std_df[col] = np.nan
    std_df = std_df[output_cols]
    log(f"清洗後資料欄位: {list(std_df.columns)}")
    log(f"清洗後資料筆數: {len(std_df)}")
    for col in std_df.columns:
        non_null_count = std_df[col].notnull().sum() - std_df[col].isna().sum()
        log(f"欄位 {col} 非空值數量: {non_null_count}")
    return std_df

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
    # 合併所有檔案
    all_dfs = []
    for f in files:
        result = process_file(f)
        if isinstance(result, pd.DataFrame):
            all_dfs.append(result)
    if not all_dfs:
        log("[ERROR] 沒有任何檔案成功處理")
        return
    merged_df = pd.concat(all_dfs, ignore_index=True)
    log(f"[INFO] 合併後總筆數: {len(merged_df)}")
    if 'order_sn' in merged_df.columns:
        before = len(merged_df)
        merged_df = merged_df.drop_duplicates(subset=['order_sn'], keep='first')
        after = len(merged_df)
        log(f"[INFO] 依 order_sn 去重後筆數: {after}（去除 {before-after} 筆重複）")
    # 取得 order_date 範圍
    min_date, max_date = None, None
    if 'order_date' in merged_df.columns:
        valid_dates = pd.to_datetime(merged_df['order_date'], errors='coerce').dropna()
        if len(valid_dates) > 0:
            min_date = valid_dates.min().strftime('%Y%m%d')
            max_date = valid_dates.max().strftime('%Y%m%d')
            log(f"[INFO] order_date 範圍: {min_date} ~ {max_date}")
    # 輸出檔案
    if min_date and max_date:
        output_filename = f'Yahoo_order_data_{min_date}_{max_date}.csv'
    else:
        output_filename = f'Yahoo_order_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = os.path.join(OUTPUT_DIR, output_filename)
    merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
    log(f"[INFO] 輸出檔案: {output_path}")
    log(f"[INFO] 最終資料筆數: {len(merged_df)}")
    log(f"[INFO] 最終資料欄位: {list(merged_df.columns)}")
    # 統計每欄非空值數量
    for col in merged_df.columns:
        non_null_count = merged_df[col].notna().sum()
        log(f"[INFO] {col}: {non_null_count}/{len(merged_df)} 筆有值")

if __name__ == '__main__':
    batch_clean() 