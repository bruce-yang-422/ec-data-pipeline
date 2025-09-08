#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物銷售報表清洗腳本

清洗 data_raw/etmall/sales_report 下所有檔案及下面資料夾內檔案
只保留指定的 14 個欄位：
- delivery_company, order_sn, seller_product_sn, product_name_platform
- quantity, unit_price, customer_name, shipping_address, customer_day_phone
- platform, note, order_amount, cost_to_platform, order_date
輸出到 temp/etmall/Sales_Report 目錄
order_date 轉換為 DATE 資料型態，其他欄位保持字串格式
"""

import sys
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import List
import re
import shutil

def setup_logging() -> None:
    """設定日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

def is_valid_column_name(column_name: str) -> bool:
    """檢查欄位名稱是否為有效的英文欄位名稱"""
    # 移除空白並轉換為字串
    column_name = str(column_name).strip()
    
    # 檢查是否為空或 NaN
    if not column_name or column_name.lower() in ['nan', 'none', 'null', '']:
        return False
    
    # 檢查是否為 Unnamed 欄位
    if 'unnamed' in column_name.lower():
        return False
    
    # 檢查是否包含英文文字（至少一個英文字母）
    if not re.search(r'[a-zA-Z]', column_name):
        return False
    
    return True

def is_valid_order_sn(order_sn: str) -> bool:
    """檢查 order_sn 是否為有效的訂單編號"""
    # 移除空白並轉換為字串
    order_sn = str(order_sn).strip()
    
    # 檢查是否為空或 NaN
    if not order_sn or order_sn.lower() in ['nan', 'none', 'null', '']:
        return False
    
    # 檢查是否為純數字（銷售報表的訂單編號通常是純數字）
    if re.match(r'^\d+$', order_sn):
        return True
    
    # 檢查是否只包含英文文字和數字
    if re.match(r'^[a-zA-Z0-9]+$', order_sn):
        return True
    
    return False

def clean_order_report_file(file_path: Path, temp_dir: Path) -> bool:
    """清洗單個訂單報表檔案"""
    try:
        logging.info(f"開始清洗檔案：{file_path.name}")
        
        # 讀取 CSV 檔案，強制所有欄位為字串類型
        df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str)
        
        # 記錄原始欄位數量
        original_columns = len(df.columns)
        logging.info(f"原始欄位數量：{original_columns}")
        
        # 銷售報表原始欄位對應（按照原始順序）
        sales_report_mapping = {
            '訂單日期': 'order_date',
            '訂單編號': 'order_sn',
            '項次': 'item_no',
            '配送狀態': 'delivery_status',
            '訂單狀態': 'order_status',
            '商品屬性': 'product_type',
            '銷售編號': 'sales_no',
            '子商品銷售編號': 'sub_sales_no',
            '子商品商品編號': 'seller_product_sn',
            '配送方式': 'delivery_method',
            '商品名稱': 'product_name_platform',
            '顏色': 'color',
            '款式': 'style',
            '售價': 'unit_price',
            '成本': 'cost_to_platform',
            '數量': 'quantity',
            '通路': 'platform',
            '配送確認日': 'delivery_confirm_date',
            '公司': 'delivery_company'
        }
        
        # 需要添加的空欄位
        additional_columns = {
            'customer_name': '',
            'shipping_address': '',
            'customer_day_phone': '',
            'note': '',
            'order_amount': ''
        }
        
        # 建立新的 DataFrame 來存放對應後的欄位，保持原始欄位順序
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
        
        # 為缺少的目標欄位添加空欄位
        for target_col, default_value in additional_columns.items():
            if target_col not in df_cleaned.columns:
                df_cleaned[target_col] = default_value
                logging.info(f"添加空欄位：{target_col}")
        
        # 保持原始欄位順序，不重新排序
        
        # 記錄清洗後的欄位數量
        cleaned_columns = len(df_cleaned.columns)
        logging.info(f"清洗後欄位數量：{cleaned_columns}")
        logging.info(f"移除欄位數量：{original_columns - cleaned_columns}")
        
        # 移除完全空白的行
        original_rows = len(df_cleaned)
        df_cleaned = df_cleaned.dropna(how='all')
        cleaned_rows = len(df_cleaned)
        
        if original_rows != cleaned_rows:
            logging.info(f"移除空白行：{original_rows - cleaned_rows} 行")
        
        # 處理空值，確保所有資料都是字串格式
        df_cleaned = df_cleaned.replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN', 'NaT'], '')
        df_cleaned = df_cleaned.fillna('')
        
        # 處理 order_date 欄位轉換為 DATE 資料型態
        if 'order_date' in df_cleaned.columns:
            try:
                # 先轉換為字串並清理
                df_cleaned['order_date'] = df_cleaned['order_date'].astype(str)
                df_cleaned['order_date'] = df_cleaned['order_date'].str.strip()
                
                # 轉換為日期格式
                df_cleaned['order_date'] = pd.to_datetime(df_cleaned['order_date'], errors='coerce')
                
                # 格式化為 YYYY-MM-DD
                df_cleaned['order_date'] = df_cleaned['order_date'].dt.strftime('%Y-%m-%d')
                
                # 將 NaT 轉換為空字串
                df_cleaned['order_date'] = df_cleaned['order_date'].fillna('')
                
                logging.info("已將 order_date 轉換為 DATE 資料型態")
            except Exception as e:
                logging.warning(f"轉換 order_date 時發生錯誤：{e}")
                # 如果轉換失敗，保持為字串格式
                df_cleaned['order_date'] = df_cleaned['order_date'].astype(str)
        
        # 其他欄位強制轉換為字串類型，避免數字自動轉換
        for col in df_cleaned.columns:
            if col != 'order_date':  # order_date 已經處理過了
                df_cleaned[col] = df_cleaned[col].astype(str)
        
        # 去除換行符號和多餘空白
        for col in df_cleaned.columns:
            df_cleaned[col] = df_cleaned[col].str.replace(r'\n|\r|\r\n', ' ', regex=True)
            df_cleaned[col] = df_cleaned[col].str.replace(r'\s+', ' ', regex=True)
            df_cleaned[col] = df_cleaned[col].str.strip()
        
        # 注意：日期時間分離已在腳本 01 中處理，此處不再需要
        
        # 檢查 order_sn 欄位並排除無效的整筆資料
        if 'order_sn' in df_cleaned.columns:
            original_rows_before_filter = len(df_cleaned)
            
            # 過濾掉 order_sn 不是英文或數字的整筆資料
            df_cleaned = df_cleaned[df_cleaned['order_sn'].apply(is_valid_order_sn)]
            
            filtered_rows = len(df_cleaned)
            removed_rows = original_rows_before_filter - filtered_rows
            
            if removed_rows > 0:
                logging.info(f"移除 order_sn 無效的資料：{removed_rows} 筆")
                logging.info(f"剩餘有效資料：{filtered_rows} 筆")
        else:
            logging.warning("未找到 order_sn 欄位，跳過 order_sn 驗證")
        
        # 建立 temp 目錄結構
        # 計算相對路徑：從 data_raw/etmall/sales_report 開始
        sales_report_index = None
        for i, part in enumerate(file_path.parts):
            if part == 'sales_report':
                sales_report_index = i
                break
        
        if sales_report_index is None:
            raise ValueError(f"無法在路徑中找到 'sales_report' 目錄：{file_path}")
        
        # 從 sales_report 之後的路徑部分（跳過 sales_report 目錄）
        relative_parts = file_path.parts[sales_report_index + 1:]
        temp_file_path = temp_dir / Path(*relative_parts)
        
        # 確保目標目錄存在
        temp_file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # 儲存清洗後的檔案到 temp 目錄，確保所有資料都是字串格式
        df_cleaned.to_csv(temp_file_path, index=False, encoding='utf-8-sig', na_rep='')
        logging.info(f"✅ 檔案清洗完成：{file_path.name}")
        logging.info(f"   輸出位置：{temp_file_path}")
        logging.info(f"   保留欄位：{list(df_cleaned.columns)}")
        logging.info(f"   資料類型：所有欄位已強制轉換為字串格式")
        logging.info(f"   最終資料筆數：{len(df_cleaned)} 筆")
        
        return True
        
    except Exception as e:
        logging.error(f"清洗檔案 {file_path.name} 時發生錯誤：{e}")
        return False

def find_sales_report_files(sales_report_dir: Path) -> List[Path]:
    """尋找 sales_report 目錄下的所有 CSV 檔案"""
    csv_files = []
    
    # 遞迴搜尋所有 CSV 檔案
    for file_path in sales_report_dir.rglob("*.csv"):
        # 排除備份檔案
        if 'backup' not in file_path.name.lower():
            csv_files.append(file_path)
    
    return csv_files

def main() -> None:
    """主函數"""
    setup_logging()
    
    # 取得專案根目錄
    project_root = Path(__file__).resolve().parents[2]
    sales_report_dir = project_root / 'data_raw' / 'etmall' / 'sales_report'
    temp_dir = project_root / 'temp' / 'etmall' / 'Sales_Report'
    
    logging.info(f'專案根目錄：{project_root}')
    logging.info(f'銷售報表目錄：{sales_report_dir}')
    logging.info(f'輸出目錄：{temp_dir}')
    
    # 檢查目錄是否存在
    if not sales_report_dir.exists():
        logging.error(f"銷售報表目錄不存在：{sales_report_dir}")
        return
    
    # 建立 temp 目錄
    temp_dir.mkdir(parents=True, exist_ok=True)
    logging.info(f"已建立輸出目錄：{temp_dir}")
    
    # 尋找所有 CSV 檔案
    csv_files = find_sales_report_files(sales_report_dir)
    
    if not csv_files:
        logging.info("沒有找到需要清洗的 CSV 檔案")
        return
    
    logging.info(f"找到 {len(csv_files)} 個 CSV 檔案需要清洗")
    
    # 清洗每個檔案
    success_count = 0
    failed_count = 0
    
    for file_path in csv_files:
        logging.info(f"\n{'='*50}")
        if clean_order_report_file(file_path, temp_dir):
            success_count += 1
        else:
            failed_count += 1
    
    # 總結
    logging.info(f"\n{'='*50}")
    logging.info("📊 清洗結果總結：")
    logging.info(f"   - 成功清洗：{success_count} 個檔案")
    logging.info(f"   - 清洗失敗：{failed_count} 個檔案")
    logging.info(f"   - 總計處理：{len(csv_files)} 個檔案")
    logging.info(f"   - 輸出目錄：{temp_dir}")
    
    if success_count > 0:
        logging.info("✅ 銷售報表清洗完成！")
    else:
        logging.error("❌ 沒有成功清洗任何檔案")
        sys.exit(1)

if __name__ == '__main__':
    main()
