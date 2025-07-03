"""
shopee_orders_to_master_cleaner.py

說明：
- 將 Shopee 匯出訂單(csv) 按 master_fields_mapping.json 定義，
  重新欄位對齊、型態處理、line_no編號、coupon_used布林化、欄位補預設值。
- 產出標準 a05_master_orders_cleaned.csv（含首行欄位名）

使用：
python shopee_orders_to_master_cleaner.py

設定檔位置：
- config/master_fields_mapping.json
- 來源 csv: data_raw/shopee/A01_master_orders_cleaned_for_bigquery.csv
- 目的 csv: data_processed/merged/a05_master_orders_cleaned.csv
"""

import pandas as pd
import json
from datetime import datetime
import os
from glob import glob
import re

MAPPING_PATH = "config/shopee_fields_mapping.json"
SOURCE_DIR = "temp/shopee"
OUTPUT_PATH = "data_processed/merged/shopee_master_orders_cleaned.csv"

def get_mapping_and_columns():
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        mapping = json.load(f)
    columns = sorted(mapping.keys(), key=lambda k: int(mapping[k]["order"]))
    en2zh = {k: v["zh_name"] for k, v in mapping.items()}
    return mapping, columns, en2zh

def parse_shop_info(filename):
    # 假設檔名格式：有才寵物商店_yutsai_petmarket_Order.all.20250603_20250703.xlsx
    # 取 shop_name, shop_account
    base = os.path.basename(filename)
    m = re.match(r"(.+?)_([\w\d]+)_Order", base)
    if m:
        return m.group(1), m.group(2)
    else:
        return "", ""

def read_all_xlsx(source_dir, en2zh):
    files = glob(os.path.join(source_dir, "*.xlsx"))
    dfs = []
    file_shopinfo = []
    for file in files:
        try:
            df = pd.read_excel(file, dtype=str).fillna("")
            shop_name, shop_account = parse_shop_info(file)
            df["__shop_name__"] = shop_name
            df["__shop_account__"] = shop_account
            dfs.append(df)
            file_shopinfo.append((file, shop_name, shop_account))
        except Exception as e:
            print(f"讀取失敗: {file}，錯誤: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return pd.DataFrame()

def main():
    mapping, columns, en2zh = get_mapping_and_columns()
    print(f"[1/4] 讀取 Shopee 訂單資料中 ...")
    df = read_all_xlsx(SOURCE_DIR, en2zh)
    if df.empty:
        print("沒有新資料，保留舊檔案不更新。")
        return

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    # 依 mapping order 重新排序，確保 item_seq 在 order_sn 後面
    columns = sorted(mapping.keys(), key=lambda k: int(mapping[k]["order"]))
    # 取得 order_sn 與 item_seq 的 index
    if "order_sn" in columns and "item_seq" in columns:
        order_sn_idx = columns.index("order_sn")
        item_seq_idx = columns.index("item_seq")
        # 若 item_seq 不在 order_sn 後面，調整順序
        if item_seq_idx != order_sn_idx + 1:
            columns.remove("item_seq")
            columns.insert(order_sn_idx + 1, "item_seq")
    # item_seq 產生邏輯：同一訂單（order_date+order_sn）下，依商品排序流水號
    group_cols = [en2zh["order_date"], en2zh["order_sn"]]
    item_sort_cols = []
    if en2zh.get("product_sku_main"):
        item_sort_cols.append(en2zh["product_sku_main"])
    if en2zh.get("product_sku_variation"):
        item_sort_cols.append(en2zh["product_sku_variation"])
    # 先依 group_cols+item_sort_cols 排序
    df = df.sort_values(group_cols + item_sort_cols).reset_index(drop=True)
    # 依 group_cols 分組，對每組給流水號
    df["__item_seq__"] = df.groupby(group_cols).cumcount() + 1
    # 建立 sku_key 欄位（主/選項貨號擇一）
    df['sku_key'] = df[en2zh['product_sku_main']].where(df[en2zh['product_sku_main']] != '', df[en2zh['product_sku_variation']])
    # 由 order_sn 前6碼解析 order_date（YYMMDD 轉 YYYY-MM-DD），並覆蓋原本 order_date 欄位
    def parse_order_date_from_sn(sn):
        if isinstance(sn, str) and len(sn) >= 6 and sn[:6].isdigit():
            y, m, d = sn[:2], sn[2:4], sn[4:6]
            year = int(y)
            year += 2000 if year < 50 else 1900  # Shopee 訂單號通常 20xx
            return f"{year:04d}-{int(m):02d}-{int(d):02d}"
        return ''
    df[en2zh['order_date']] = df[en2zh['order_sn']].apply(parse_order_date_from_sn)
    df['order_date_only'] = df[en2zh['order_date']]
    # 建立唯一鍵欄位 key_for_merge
    df['key_for_merge'] = df['order_date_only'].astype(str) + '_' + df[en2zh['order_sn']].astype(str) + '_' + df['sku_key'].astype(str)

    # 產生 rows 時，sku_key 也要填入
    rows = []
    for idx, row in df.iterrows():
        new_row = []
        for col in columns:
            if col == "processing_date":
                new_row.append(now_str)
            elif col == "shop_name":
                new_row.append(row.get("__shop_name__", ""))
            elif col == "shop_account":
                new_row.append(row.get("__shop_account__", ""))
            elif col == "item_seq":
                new_row.append("")  # 先不填，合併後再補
            else:
                zh_col = en2zh.get(col, None)
                if zh_col and zh_col in row:
                    new_row.append(row[zh_col])
                else:
                    new_row.append("")
        # sku_key、key_for_merge 放最後
        new_row.append(row['sku_key'])
        new_row.append(row['key_for_merge'])
        rows.append(new_row)
        percent = int((idx + 1) / len(df) * 100)
        print(f"\r  轉換進度：{percent}% ({idx + 1}/{len(df)})", end='', flush=True)
    # 欄位順序：原本 columns + sku_key + key_for_merge
    out_df = pd.DataFrame(rows, columns=columns + ["sku_key", "key_for_merge"])

    # 合併時以 key_for_merge 為唯一鍵
    unique_cols = ["key_for_merge"]
    if os.path.exists(OUTPUT_PATH):
        old_df = pd.read_csv(OUTPUT_PATH, dtype=str).fillna("")
        combined = pd.concat([old_df, out_df], ignore_index=True)
        combined = combined.drop_duplicates(subset=unique_cols, keep="last")
    else:
        combined = out_df

    # 重新依同一訂單分組排序 item_seq
    combined = combined.sort_values(["order_date", "order_sn", "sku_key"]).reset_index(drop=True)
    combined["item_seq"] = combined.groupby(["order_date", "order_sn"]).cumcount() + 1

    # 過濾掉 order_date 與 order_sn 都為空的 row
    combined = combined[~((combined["order_date"] == "") & (combined["order_sn"] == ""))]

    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    combined.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")
    print(f"\r[4/4] 轉換完成！總共 {len(combined)} 筆資料已存入：{OUTPUT_PATH}         ")

if __name__ == "__main__":
    main()
