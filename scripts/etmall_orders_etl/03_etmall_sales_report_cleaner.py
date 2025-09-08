#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物銷售報表清洗腳本
清洗 data_raw\etmall\sales_report 下所有 CSV 檔案，輸出到 temp\etmall\Sales_Report
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import re

# 設定專案根目錄
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / 'etmall_sales_report_cleaner.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

def find_sales_report_files(sales_report_dir: Path) -> list:
    """
    尋找所有銷售報表 CSV 檔案
    
    Args:
        sales_report_dir: 銷售報表目錄
        
    Returns:
        list: CSV 檔案路徑列表
    """
    csv_files = []
    
    if not sales_report_dir.exists():
        logging.warning(f"銷售報表目錄不存在：{sales_report_dir}")
        return csv_files
    
    # 遞歸搜尋所有 CSV 檔案
    for file_path in sales_report_dir.rglob("*.csv"):
        if file_path.is_file():
            csv_files.append(file_path)
            logging.info(f"找到銷售報表檔案：{file_path}")
    
    return sorted(csv_files)

def clean_sales_report_file(file_path: Path, temp_dir: Path) -> bool:
    """
    清洗單一銷售報表檔案
    
    Args:
        file_path: 原始檔案路徑
        temp_dir: 輸出目錄
        
    Returns:
        bool: 是否成功
    """
    try:
        logging.info(f"開始清洗檔案：{file_path.name}")
        
        # 讀取 CSV 檔案，強制所有欄位為字串類型
        df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str)
        
        # 自動處理換行符號轉換
        # 將所有欄位中的換行符號完全移除，確保 CSV 格式正確
        for col in df.columns:
            if df[col].dtype == 'object':  # 只處理字串欄位
                df[col] = df[col].astype(str)
                # 完全移除所有換行符號
                df[col] = df[col].str.replace('\r\n', '', regex=False)
                df[col] = df[col].str.replace('\n', '', regex=False)
                df[col] = df[col].str.replace('\r', '', regex=False)
                df[col] = df[col].str.replace('\t', ' ', regex=False)
                # 將多個連續空格替換為單一空格
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
                # 去除欄位前後空白
                df[col] = df[col].str.strip()
        
        logging.info("已自動處理換行符號轉換（完全移除換行符號）")
        
        # 記錄原始欄位數量
        original_columns = len(df.columns)
        logging.info(f"原始欄位數量：{original_columns}")
        
        # 銷售報表原始欄位對應（按照 etmall_fields_mapping.json 的順序）
        sales_report_mapping = {
            '訂單日期': 'order_date',
            '訂單編號': 'order_sn',
            '項次': 'item_no',
            '配送狀態': 'shipping_status',
            '訂單狀態': 'order_status',
            '商品屬性': 'product_attribute',
            '銷售編號': 'product_sale_id',
            '子商品銷售編號': 'sub_sale_id',
            '子商品商品編號': 'sub_product_id',
            '配送方式': 'shipping_method',
            '商品名稱': 'product_name_platform',
            '顏色': 'color',
            '款式': 'style',
            '售價': 'unit_price',
            '成本': 'cost_to_platform',
            '數量': 'quantity',
            '通路': 'channel',
            '配送確認日': 'shipping_confirm_date'
        }
        
        # 需要添加的空欄位（按照 etmall_fields_mapping.json 的順序）
        additional_columns = {
            'platform': 'etmall',
            'order_time': '',
            'merge_no': '',
            'order_type': '',
            'order_type_code': '',
            'shipping_sn': '',
            'shipping_carrier': '',
            'shipping_code': '',
            'shipping_request_date': '',
            'shipping_expected_date': '',
            'shipping_expected_time': '',
            'product_id': '',
            'seller_product_sn': '',
            'customer_name': '',
            'customer_phone': '',
            'customer_tel': '',
            'shipping_address': '',
            'note': '',
            'gift_info': '',
            'vendor_shipping_note': '',
            'expected_stockin_date': '',
            'expected_delivery_date': '',
            'channel_type': '',
            'shop_id': '',
            'shop_name': '',
            'shop_business_model': '',
            'location': '',
            'department': '',
            'manager': '',
            'category_level_1': '',
            'category_level_2': '',
            'brand': '',
            'series': '',
            'pet_type': '',
            'product_name': '',
            'item_code': '',
            'sku': '',
            'tags': '',
            'spec': '',
            'unit': '',
            'origin': '',
            'supplier_code': '',
            'supplier': '',
            'purchase_cost': ''
        }
        
        # 定義完整的欄位順序（按照 etmall_fields_mapping.json 的 order 順序）
        field_order = [
            'platform', 'order_date', 'order_time', 'order_sn', 'item_no', 'order_line_uid', 'merge_no',
            'shipping_status', 'order_status', 'order_type', 'order_type_code', 'shipping_sn', 'shipping_carrier',
            'shipping_code', 'shipping_method', 'shipping_request_date', 'shipping_expected_date', 'shipping_expected_time',
            'shipping_confirm_date', 'product_sale_id', 'sub_sale_id', 'product_id', 'sub_product_id', 'product_name_platform',
            'color', 'style', 'product_attribute', 'seller_product_sn', 'quantity', 'unit_price', 'cost_to_platform',
            'purchase_cost', 'customer_name', 'customer_phone', 'customer_tel', 'shipping_address', 'note', 'gift_info',
            'vendor_shipping_note', 'expected_stockin_date', 'expected_delivery_date', 'channel_type', 'channel',
            'shop_id', 'shop_name', 'shop_business_model', 'location', 'department', 'manager', 'category_level_1',
            'category_level_2', 'brand', 'series', 'pet_type', 'product_name', 'item_code', 'sku', 'tags', 'spec',
            'unit', 'origin', 'supplier_code', 'supplier'
        ]
        
        # 建立新的 DataFrame 來存放對應後的欄位，按照 etmall_fields_mapping.json 的順序
        df_cleaned = pd.DataFrame()
        
        # 按照原始銷售報表的欄位順序進行對應
        for original_col in df.columns:
            # 檢查是否有對應的目標欄位
            target_col = sales_report_mapping.get(original_col)
            
            if target_col:
                df_cleaned[target_col] = df[original_col]
                logging.info(f"對應欄位：{original_col} -> {target_col}")
            else:
                # 如果沒有對應的目標欄位，跳過此欄位
                logging.info(f"跳過欄位：{original_col}")
        
        # 添加缺少的欄位
        for target_col, default_value in additional_columns.items():
            if target_col not in df_cleaned.columns:
                df_cleaned[target_col] = default_value
                logging.info(f"添加空欄位：{target_col}")
        
        # 自動產生 order_line_uid = order_sn + "_" + item_no
        if 'order_sn' in df_cleaned.columns and 'item_no' in df_cleaned.columns:
            df_cleaned['order_line_uid'] = df_cleaned['order_sn'].astype(str) + "_" + df_cleaned['item_no'].astype(str)
            logging.info("自動產生 order_line_uid 欄位")
        
        # 設定 supplier 欄位為空白
        if 'supplier' in df_cleaned.columns:
            df_cleaned['supplier'] = ''
            logging.info("設定 supplier 欄位為空白")
        
        # 處理 item_no 格式（個位數前面補0）
        if 'item_no' in df_cleaned.columns:
            df_cleaned['item_no'] = df_cleaned['item_no'].astype(str).str.zfill(2)
            logging.info("已處理 item_no 格式（個位數前面補0）")
        
        # 按照 field_order 重新排序欄位
        df_cleaned = df_cleaned[field_order]
        
        # 按照 order_sn 和 item_no 排序
        if 'order_sn' in df_cleaned.columns and 'item_no' in df_cleaned.columns:
            # 轉換為數值型態進行排序
            df_cleaned['order_sn_numeric'] = pd.to_numeric(df_cleaned['order_sn'], errors='coerce')
            df_cleaned['item_no_numeric'] = pd.to_numeric(df_cleaned['item_no'], errors='coerce')
            
            # 排序
            df_cleaned = df_cleaned.sort_values(['order_sn_numeric', 'item_no_numeric'], ascending=[True, True])
            
            # 移除臨時欄位
            df_cleaned = df_cleaned.drop(['order_sn_numeric', 'item_no_numeric'], axis=1)
            
            logging.info("已按照 order_sn 和 item_no 排序（由小到大）")
        
        # 記錄清洗後的欄位數量
        cleaned_columns = len(df_cleaned.columns)
        logging.info(f"清洗後欄位數量：{cleaned_columns}")
        logging.info(f"移除欄位數量：{original_columns - cleaned_columns}")
        
        # 將 order_date 轉換為 DATE 資料型態
        if 'order_date' in df_cleaned.columns:
            df_cleaned['order_date'] = pd.to_datetime(df_cleaned['order_date'], errors='coerce').dt.strftime('%Y-%m-%d')
            logging.info("已將 order_date 轉換為 DATE 資料型態")
        
        # 建立輸出目錄結構
        sales_report_index = None
        for i, part in enumerate(file_path.parts):
            if part == 'sales_report':
                sales_report_index = i
                break
        
        if sales_report_index is not None:
            relative_parts = file_path.parts[sales_report_index + 1:]
            temp_file_path = temp_dir / Path(*relative_parts)
        else:
            temp_file_path = temp_dir / file_path.name
        
        # 建立輸出目錄
        temp_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 儲存清洗後的檔案
        df_cleaned.to_csv(temp_file_path, index=False, encoding='utf-8-sig')
        
        logging.info(f"✅ 檔案清洗完成：{file_path.name}")
        logging.info(f"   輸出位置：{temp_file_path}")
        logging.info(f"   保留欄位：{list(df_cleaned.columns)}")
        logging.info(f"   資料類型：所有欄位已強制轉換為字串格式")
        logging.info(f"   最終資料筆數：{len(df_cleaned)} 筆")
        
        return True
        
    except Exception as e:
        logging.error(f"清洗檔案失敗：{file_path.name} - {str(e)}")
        return False

def main():
    """主函數"""
    logging.info("=" * 50)
    logging.info("開始執行東森購物銷售報表清洗腳本")
    logging.info("=" * 50)
    
    # 設定路徑
    sales_report_dir = project_root / "data_raw" / "etmall" / "sales_report"
    temp_dir = project_root / "temp" / "etmall" / "Sales_Report"
    
    logging.info(f"專案根目錄：{project_root}")
    logging.info(f"銷售報表目錄：{sales_report_dir}")
    logging.info(f"輸出目錄：{temp_dir}")
    logging.info(f"已建立輸出目錄：{temp_dir}")
    
    # 建立輸出目錄
    temp_dir.mkdir(parents=True, exist_ok=True)
    
    # 尋找所有 CSV 檔案
    csv_files = find_sales_report_files(sales_report_dir)
    
    if not csv_files:
        logging.error("沒有找到任何 CSV 檔案，程式結束")
        return
    
    logging.info(f"找到 {len(csv_files)} 個 CSV 檔案需要清洗")
    
    # 清洗每個檔案
    success_count = 0
    for file_path in csv_files:
        logging.info("-" * 50)
        if clean_sales_report_file(file_path, temp_dir):
            success_count += 1
    
    logging.info("=" * 50)
    logging.info("📊 清洗結果總結：")
    logging.info(f"   - 成功清洗：{success_count} 個檔案")
    logging.info(f"   - 清洗失敗：{len(csv_files) - success_count} 個檔案")
    logging.info(f"   - 總計處理：{len(csv_files)} 個檔案")
    logging.info(f"   - 輸出目錄：{temp_dir}")
    logging.info("✅ 銷售報表清洗完成！")
    logging.info("=" * 50)

if __name__ == "__main__":
    main()
