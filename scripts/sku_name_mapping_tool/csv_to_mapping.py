import os
import pandas as pd
import json
import glob
from datetime import datetime
import re

# 路徑設定
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(os.path.dirname(SCRIPT_DIR))
CONFIG_DIR = os.path.join(PROJECT_ROOT, 'config')
LOG_DIR = os.path.join(PROJECT_ROOT, 'logs')

# 確保目錄存在
os.makedirs(CONFIG_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def find_latest_merged_products():
    """尋找最新的 merged_products CSV 檔案"""
    pattern = os.path.join(SCRIPT_DIR, 'merged_products_*.csv')
    files = glob.glob(pattern)
    
    if not files:
        raise FileNotFoundError(f"找不到 merged_products CSV 檔案於 {SCRIPT_DIR}")
    
    # 依檔案修改時間排序，取最新的
    latest_file = max(files, key=os.path.getmtime)
    print(f"找到最新檔案: {latest_file}")
    return latest_file

def clean_text(text):
    """清洗文字資料"""
    if pd.isna(text) or text == '':
        return ''
    
    text = str(text).strip()
    # 去除特殊符號和空白
    text = re.sub(r'[^\w\s\u4e00-\u9fff\-\.]', '', text)
    return text

def is_valid_barcode(barcode):
    """檢查條碼是否為英文字母或數字，排除含有中文的條碼"""
    if pd.isna(barcode) or str(barcode).strip() == '':
        return False
    barcode_str = str(barcode).strip()
    
    # 檢查是否包含中文字符
    if re.search(r'[\u4e00-\u9fff]', barcode_str):
        return False
    
    # 檢查是否只包含英文字母、數字和常見符號
    if re.match(r'^[a-zA-Z0-9\-_\.]+$', barcode_str):
        return True
    
    return False

def create_barcode_mapping(df):
    """建立以條碼為 key 的 mapping"""
    mapping = {}
    skipped_count = 0
    
    for idx, row in df.iterrows():
        # 檢查條碼是否存在且為有效格式（英文字母或數字，不含中文）
        if not is_valid_barcode(row['barcode']):
            skipped_count += 1
            continue
        
        barcode = str(row['barcode']).strip()
        
        # 決定主要識別碼 (sku 優先，其次 item_code)
        primary_key = None
        if pd.notna(row['sku']) and str(row['sku']).strip() != '':
            primary_key = str(row['sku']).strip()
        elif pd.notna(row['item_code']) and str(row['item_code']).strip() != '':
            primary_key = str(row['item_code']).strip()
        else:
            skipped_count += 1
            continue  # 跳過沒有識別碼的記錄
        
        # 建立 mapping 資料
        mapping_data = {
            'sku': primary_key,
            'product_name': clean_text(row['product_name']),
            'brand': clean_text(row['brand']),
            'category': clean_text(row['category']),
            'subcategory': clean_text(row['subcategory']),
            'series': clean_text(row['series']),
            'pet_type': clean_text(row['pet_type']),
            'spec': clean_text(row['spec']),
            'unit': clean_text(row['unit']),
            'package_type': clean_text(row['package_type']),
            'package_qty': clean_text(row['package_qty']),
            'origin': clean_text(row['origin']),
            'tags': clean_text(row['tags']),
            'supplier': clean_text(row['supplier']),
            'supplier_code': clean_text(row['supplier_code']),
            'status': clean_text(row['status']),
            'stock_status': clean_text(row['stock_status']),
            'location': clean_text(row['location']),
            'msrp': float(row['msrp']) if pd.notna(row['msrp']) and str(row['msrp']).strip() != '' else None,
            'price': float(row['price']) if pd.notna(row['price']) and str(row['price']).strip() != '' else None,
            'supplier_price': float(row['supplier_price']) if pd.notna(row['supplier_price']) and str(row['supplier_price']).strip() != '' else None,
            'list_price': float(row['list_price']) if pd.notna(row['list_price']) and str(row['list_price']).strip() != '' else None,
            'cost': float(row['cost']) if pd.notna(row['cost']) and str(row['cost']).strip() != '' else None,
            'amount': float(row['amount']) if pd.notna(row['amount']) and str(row['amount']).strip() != '' else None,
            'min_qty': int(row['min_qty']) if pd.notna(row['min_qty']) and str(row['min_qty']).strip() != '' else None,
            'source_file': clean_text(row['source_file']),
            'updated_at': clean_text(row['updated_at']),
            'notes': clean_text(row['notes'])
        }
        
        # 移除空值
        mapping_data = {k: v for k, v in mapping_data.items() if v is not None and v != ''}
        
        mapping[barcode] = mapping_data
    
    print(f"跳過無效條碼（含中文或無識別碼）的記錄: {skipped_count} 筆")
    return mapping

def create_sku_mapping(df):
    """建立 SKU 對應的 mapping (反向查詢用)"""
    mapping = {}
    
    for idx, row in df.iterrows():
        # 決定主要識別碼
        primary_key = None
        if pd.notna(row['sku']) and str(row['sku']).strip() != '':
            primary_key = str(row['sku']).strip()
        elif pd.notna(row['item_code']) and str(row['item_code']).strip() != '':
            primary_key = str(row['item_code']).strip()
        else:
            continue
        
        # 只記錄有有效條碼的項目
        if is_valid_barcode(row['barcode']):
            barcode = str(row['barcode']).strip()
            mapping[primary_key] = barcode
    
    return mapping

def create_product_name_mapping(df):
    """建立商品名稱對應的 mapping"""
    mapping = {}
    
    for idx, row in df.iterrows():
        if pd.notna(row['product_name']) and str(row['product_name']).strip() != '':
            product_name = clean_text(row['product_name'])
            
            # 只記錄有有效條碼的項目
            if not is_valid_barcode(row['barcode']):
                continue
            
            barcode = str(row['barcode']).strip()
            
            # 如果商品名稱已存在，建立列表
            if product_name in mapping:
                if isinstance(mapping[product_name], list):
                    if barcode not in mapping[product_name]:
                        mapping[product_name].append(barcode)
                else:
                    if mapping[product_name] != barcode:
                        mapping[product_name] = [mapping[product_name], barcode]
            else:
                mapping[product_name] = barcode
    
    return mapping

def main():
    """主要處理函數"""
    try:
        # 尋找最新檔案
        csv_file = find_latest_merged_products()
        
        # 讀取 CSV 檔案
        print(f"讀取檔案: {csv_file}")
        df = pd.read_csv(csv_file, dtype=str)
        print(f"原始資料筆數: {len(df)}")
        
        # 移除完全空白的行
        df = df.dropna(how='all')
        print(f"移除空白行後筆數: {len(df)}")
        
        # 建立各種 mapping
        print("建立條碼 mapping (主要)...")
        barcode_mapping = create_barcode_mapping(df)
        print(f"條碼 mapping 筆數: {len(barcode_mapping)}")
        
        print("建立 SKU mapping (反向查詢)...")
        sku_mapping = create_sku_mapping(df)
        print(f"SKU mapping 筆數: {len(sku_mapping)}")
        
        print("建立商品名稱 mapping...")
        product_name_mapping = create_product_name_mapping(df)
        print(f"商品名稱 mapping 筆數: {len(product_name_mapping)}")
        
        # 建立完整的 mapping 結構
        mapping_data = {
            'metadata': {
                'created_at': datetime.now().isoformat(),
                'source_file': os.path.basename(csv_file),
                'total_records': len(df),
                'barcode_mapping_count': len(barcode_mapping),
                'sku_mapping_count': len(sku_mapping),
                'product_name_mapping_count': len(product_name_mapping),
                'note': '以條碼為主要 key，僅包含英文字母或數字的條碼（排除中文）'
            },
            'barcode_mapping': barcode_mapping,
            'sku_mapping': sku_mapping,
            'product_name_mapping': product_name_mapping
        }
        
        # 輸出 JSON 檔案
        output_file = os.path.join(CONFIG_DIR, 'sku_mapping.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(mapping_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 成功建立 mapping 檔案: {output_file}")
        print(f"📊 統計資訊:")
        print(f"   - 原始資料筆數: {len(df)}")
        print(f"   - 條碼 mapping (主要): {len(barcode_mapping)} 筆")
        print(f"   - SKU mapping (反向查詢): {len(sku_mapping)} 筆")
        print(f"   - 商品名稱 mapping: {len(product_name_mapping)} 筆")
        
        # 寫入 log
        log_file = os.path.join(LOG_DIR, 'sku_mapping_generation.log')
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 成功建立 mapping 檔案: {output_file}\n")
            f.write(f"  原始資料筆數: {len(df)}, 條碼 mapping: {len(barcode_mapping)}, SKU mapping: {len(sku_mapping)}, 商品名稱 mapping: {len(product_name_mapping)}\n")
        
    except Exception as e:
        print(f"❌ 錯誤: {e}")
        # 寫入錯誤 log
        log_file = os.path.join(LOG_DIR, 'sku_mapping_generation.log')
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 錯誤: {e}\n")

if __name__ == '__main__':
    main() 