"""
===============================================================================
MOMO SKU 資料豐富化工具 (MOMO SKU Data Enrichment Tool)
===============================================================================

📋 腳本用途：
    本腳本用於為 MOMO 購物中心的訂單資料增加商品相關資訊，透過條碼對應表
    自動填入商品分類、品牌、規格等詳細資訊，提升資料完整性和分析價值。

🎯 核心重點：
    1. 智能條碼匹配：支援多個條碼欄位的自動識別和匹配
    2. 商品資訊豐富：自動填入 18 個商品相關欄位
    3. 高匹配率：透過條碼對應表實現精確的商品資訊匹配
    4. 批次處理：支援多個 MOMO CSV 檔案的批次處理

🔧 主要功能：
    - 載入 SKU mapping 配置檔案（config/sku_mapping.json）
    - 自動掃描 data_processed/merged 目錄下的 MOMO CSV 檔案
    - 透過條碼欄位匹配商品主檔資訊
    - 自動填入商品分類、品牌、規格等詳細資訊
    - 生成豐富化後的 CSV 檔案（檔名後綴 _enriched）
    - 提供詳細的匹配統計和處理報告

📊 資料豐富化欄位：
    商品分類：category, subcategory
    品牌資訊：brand, series
    商品屬性：pet_type, product_name_mapped, item_code, sku
    商品標籤：tags, spec, unit, package_type, package_qty
    供應商資訊：origin, cost, supplier_code, supplier, supplier_ref

🔍 條碼匹配邏輯：
    1. 優先檢查 product_manufacturer_code 欄位
    2. 其次檢查 barcode 欄位
    3. 最後檢查 product_sku_main 欄位
    4. 任一欄位匹配成功即填入對應商品資訊

🚀 使用場景：
    - MOMO 訂單資料品質提升
    - 商品資訊自動化豐富
    - 電商資料分析準備
    - 商品主檔資料整合

📁 輸入檔案：
    - 位置：data_processed/merged/momo_*.csv
    - 要求：必須包含條碼相關欄位
    - 配置：config/sku_mapping.json（條碼對應表）

📈 輸出結果：
    - 豐富化後的 CSV 檔案（檔名後綴 _enriched）
    - 控制台即時顯示處理進度和匹配統計
    - 詳細的操作日誌（logs/momo_sku_enrichment.log）
    - 整體匹配率和處理統計摘要

⚙️ 配置要求：
    - sku_mapping.json 必須包含 barcode_mapping 結構
    - 每個條碼對應完整的商品資訊字典
    - 支援的欄位：category, subcategory, brand, series, pet_type 等

作者：EC Data Pipeline 團隊
版本：v1.0.0
更新日期：2025-08-19
===============================================================================
"""

import os
import pandas as pd
import json
import glob
from datetime import datetime
from pathlib import Path

# 路徑設定
PROJECT_ROOT = Path(__file__).parent.parent
DATA_PROCESSED_DIR = PROJECT_ROOT / 'data_processed' / 'merged'
CONFIG_DIR = PROJECT_ROOT / 'config'
LOG_DIR = PROJECT_ROOT / 'logs'

# 確保目錄存在
os.makedirs(LOG_DIR, exist_ok=True)

def load_sku_mapping():
    """載入 SKU mapping 資料"""
    mapping_file = CONFIG_DIR / 'sku_mapping.json'
    
    if not mapping_file.exists():
        raise FileNotFoundError(f"找不到 SKU mapping 檔案：{mapping_file}")
    
    with open(mapping_file, 'r', encoding='utf-8') as f:
        mapping_data = json.load(f)
    
    print(f"✅ 成功載入 SKU mapping，包含 {len(mapping_data['barcode_mapping'])} 筆條碼資料")
    return mapping_data

def find_momo_files():
    """尋找 momo CSV 檔案"""
    pattern = DATA_PROCESSED_DIR / 'momo_*.csv'
    files = glob.glob(str(pattern))
    
    # 排除已經處理過的 _enriched.csv 檔案
    files = [f for f in files if not Path(f).name.endswith('_enriched.csv')]
    
    if not files:
        raise FileNotFoundError(f"找不到 momo CSV 檔案於 {DATA_PROCESSED_DIR}")
    
    print(f"📁 找到 {len(files)} 個 momo CSV 檔案：")
    for f in files:
        print(f"   - {Path(f).name}")
    
    return files

def get_barcode_from_row(row, mapping_data):
    """從資料行中取得條碼，並回傳對應的商品資訊"""
    # 嘗試不同的條碼欄位
    barcode_fields = ['product_manufacturer_code', 'barcode', 'product_sku_main']
    
    for field in barcode_fields:
        if field in row and pd.notna(row[field]) and str(row[field]).strip() != '':
            barcode = str(row[field]).strip()
            
            # 在 mapping 中尋找對應的商品資訊
            if barcode in mapping_data['barcode_mapping']:
                return mapping_data['barcode_mapping'][barcode]
    
    return None

def enrich_momo_data(file_path, mapping_data):
    """為 momo 資料增加欄位"""
    print(f"\n📖 處理檔案：{Path(file_path).name}")
    
    # 讀取 CSV 檔案
    df = pd.read_csv(file_path, dtype=str)
    print(f"📊 原始資料筆數：{len(df)}")
    
    # 要增加的欄位（移除指定的欄位）
    new_columns = [
        'category', 'subcategory', 'brand', 'series', 'pet_type', 
        'product_name_mapped', 'item_code', 'sku', 'tags', 'spec', 
        'unit', 'package_type', 'package_qty', 'origin', 'cost', 
        'supplier_code', 'supplier', 'supplier_ref'
    ]
    
    # 初始化新欄位
    for col in new_columns:
        df[col] = ''
    
    # 統計資訊
    matched_count = 0
    unmatched_count = 0
    
    # 逐行處理
    for idx, row in df.iterrows():
        product_info = get_barcode_from_row(row, mapping_data)
        
        if product_info:
            matched_count += 1
            # 填入對應的商品資訊
            for col in new_columns:
                if col in product_info and product_info[col] is not None:
                    df.at[idx, col] = str(product_info[col])
        else:
            unmatched_count += 1
    
    print(f"✅ 匹配成功：{matched_count} 筆")
    print(f"❌ 未匹配：{unmatched_count} 筆")
    print(f"📈 匹配率：{matched_count/(matched_count+unmatched_count)*100:.1f}%")
    
    return df

def save_enriched_data(df, original_file_path):
    """儲存增加欄位後的資料"""
    # 建立新的檔案名稱
    original_path = Path(original_file_path)
    new_filename = f"{original_path.stem}_enriched{original_path.suffix}"
    output_path = original_path.parent / new_filename
    
    # 儲存檔案
    df.to_csv(output_path, index=False, encoding='utf-8-sig')
    print(f"💾 已儲存：{output_path.name}")
    
    return output_path

def main():
    """主要處理函數"""
    try:
        print("🚀 開始處理 momo 資料增加欄位...")
        
        # 載入 SKU mapping
        mapping_data = load_sku_mapping()
        
        # 尋找 momo 檔案
        momo_files = find_momo_files()
        
        # 處理每個檔案
        processed_files = []
        total_matched = 0
        total_unmatched = 0
        
        for file_path in momo_files:
            try:
                # 增加欄位
                enriched_df = enrich_momo_data(file_path, mapping_data)
                
                # 儲存結果
                output_path = save_enriched_data(enriched_df, file_path)
                processed_files.append(output_path)
                
                # 統計匹配數量
                matched_count = len(enriched_df[enriched_df['sku'] != ''])
                unmatched_count = len(enriched_df[enriched_df['sku'] == ''])
                total_matched += matched_count
                total_unmatched += unmatched_count
                
            except Exception as e:
                print(f"❌ 處理檔案 {Path(file_path).name} 時發生錯誤：{e}")
                continue
        
        # 輸出總結
        print(f"\n🎉 處理完成！")
        print(f"📁 處理檔案數：{len(processed_files)}")
        print(f"📊 總匹配筆數：{total_matched}")
        print(f"📊 總未匹配筆數：{total_unmatched}")
        print(f"📈 整體匹配率：{total_matched/(total_matched+total_unmatched)*100:.1f}%")
        
        # 寫入 log
        log_file = LOG_DIR / 'momo_sku_enrichment.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 處理完成\n")
            f.write(f"  處理檔案數：{len(processed_files)}, 總匹配：{total_matched}, 總未匹配：{total_unmatched}\n")
        
        print(f"📝 詳細記錄已寫入：{log_file}")
        
    except Exception as e:
        print(f"❌ 錯誤：{e}")
        # 寫入錯誤 log
        log_file = LOG_DIR / 'momo_sku_enrichment.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 錯誤：{e}\n")

if __name__ == '__main__':
    main() 