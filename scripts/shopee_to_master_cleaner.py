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

MAPPING_PATH = "config/master_fields_mapping.json"
SOURCE_PATH = "data_raw/shopee/A01_master_orders_cleaned_for_bigquery.csv"
OUTPUT_PATH = "data_processed/merged/a05_master_orders_cleaned.csv"

# Shopee 欄位名 → master 欄位名對照
shopee_to_master = {
    "platform": None,
    "shop_name": "shop_name",
    "shop_account": "shop_account",
    "order_date": "order_date",
    "order_id": "order_sn",
    "line_no": "line_no",
    "order_status": "order_status",
    "buyer_name": "",  # 無
    "buyer_account": "buyer_username",
    "total_order_amount": "product_total_price",
    "buyer_paid_amount": "total_amount_paid_by_buyer",
    "account_income_amount": "",  # 無
    "product_name": "product_name",
    "product_option_name": "product_variation",
    "product_sku": "product_sku_main",
    "option_sku": "product_sku_variation",
    "quantity": "quantity",
    "product_amount": "product_original_price",
    "product_promo_amount": "product_campaign_price",
    "product_cost": "",  # 預設0
    "coupon_used": "",  # voucher_code/voucher
    "recipient_name": "recipient_name",
    "recipient_phone": "recipient_phone",
    "recipient_address": "recipient_address",
    "shipping_fee": "buyer_paid_shipping_fee",
    "payment_method": "payment_method",
    "shipping_method": "shipping_method",
    "invoice_number": "",  # 無
    "invoice_date": "",    # 無
    "ship_date": "actual_shipping_timestamp",
    "shipping_provider": "shipping_provider",
    "cancel_reason": "cancellation_reason",
    "buyer_note": "buyer_note",
    "seller_note": "seller_note",
    "etl_timestamp": None
}

def clean_recipient_name(name):
    if str(name).strip() == "****":
        return ""
    return str(name).strip()

def parse_coupon_used(row):
    val = row.get("voucher_code", "") or row.get("voucher", "")
    return True if pd.notna(val) and str(val).strip() != "" and str(val).strip() != "0" else False

def main():
    # 讀取 master 欄位
    with open(MAPPING_PATH, "r", encoding="utf-8") as f:
        master_map = json.load(f)
    master_cols = list(master_map.keys())

    print(f"[1/4] 讀取 Shopee 訂單資料中 ...")
    df = pd.read_csv(SOURCE_PATH, dtype=str).fillna("")

    # line_no: 同 order_sn 自動流水號
    print(f"[2/4] 處理 line_no ...")
    line_no_dict = {}
    line_nos = []
    for sn in df['order_sn']:
        if sn not in line_no_dict:
            line_no_dict[sn] = 1
        else:
            line_no_dict[sn] += 1
        line_nos.append(line_no_dict[sn])
    df['line_no'] = line_nos

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    total = len(df)
    rows = []

    print(f"[3/4] 轉換主表欄位格式中 ...")
    for idx, row in df.iterrows():
        new_row = []
        for col in master_cols:
            shopee_col = shopee_to_master.get(col, "")
            if col == "platform":
                new_row.append("Shopee")
            elif col == "etl_timestamp":
                new_row.append(now_str)
            elif col == "recipient_name":
                new_row.append(clean_recipient_name(row.get("recipient_name", "")))
            elif col == "product_cost":
                new_row.append(0)
            elif col == "coupon_used":
                new_row.append(parse_coupon_used(row))
            elif shopee_col:
                new_row.append(row.get(shopee_col, ""))
            else:
                new_row.append("")  # 沒有 mapping 的欄位預設空值
        rows.append(new_row)
        # 進度百分比一行顯示
        percent = int((idx + 1) / total * 100)
        print(f"\r  轉換進度：{percent}% ({idx + 1}/{total})", end='', flush=True)

    out_df = pd.DataFrame(rows, columns=master_cols)
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    out_df.to_csv(OUTPUT_PATH, index=False, encoding="utf-8-sig")

    print(f"\r[4/4] 轉換完成！總共 {total} 筆資料已存入：{OUTPUT_PATH}         ")

if __name__ == "__main__":
    main()
