# scripts/excel2mapping.py
# -*- coding: utf-8 -*-
"""
腳本用途：
--------
將 config/mapping.xlsx 內所有分頁(sheet)，
依每一分頁的「欄位定義表」自動轉出對應的 json mapping 檔，
每個分頁對應一份 {sheet}_fields_mapping.json

使用場景：
- 各平台/主表欄位定義全部集中在一份 mapping.xlsx，不用分散管理
- 每分頁維護「order, 欄位英文, 欄位中文, 型態, 說明, 是否必填, 備註」
- 部分工作表可包含額外欄位如「來源」
- 新增平台或 schema，只要多加一個分頁即可

適用流程：
- ETL 資料校驗、型態轉換、mapping 自動對照
- 資料庫建表/查帳/欄位比對
- 資料治理欄位定義單一來源（single source of truth）

表頭需求：
-----------
每一分頁需有以下核心欄位（順序不限，名稱需完全相同）：
    order, 欄位英文, 欄位中文, 型態, 說明, 是否必填, 備註
    
可選額外欄位：
    來源 (用於標記欄位來源類型)

輸出範例（momo_fields_mapping.json）：
----------------------------------------
{
    "order_id": {
        "order": "1",
        "zh_name": "訂單編號",
        "type": "STRING",
        "description": "MOMO原始訂單編號",
        "required": "是",
        "note": "",
        "source": "原始欄位"  // 如果有來源欄位
    },
    ...
}

更新記錄：
---------
- 支援處理額外的「來源」欄位
- 自動跳過空的工作表
- 改進錯誤處理和日誌輸出

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import os
import pandas as pd
import json
from pathlib import Path
import logging

# ===== 參數設定 =====
# 取得腳本所在目錄的上層目錄作為專案根目錄
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
CONFIG_DIR = PROJECT_ROOT / 'config'
LOGS_DIR = PROJECT_ROOT / 'logs'

MAPPING_XLSX = CONFIG_DIR / 'mapping.xlsx'
OUTPUT_TEMPLATE = CONFIG_DIR / '{}_fields_mapping.json'

# 確保目錄存在
LOGS_DIR.mkdir(exist_ok=True)

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOGS_DIR / 'excel2mapping.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ===== 必要欄位定義 =====
REQUIRED_COLUMNS = ['order', '欄位英文', '欄位中文', '型態', '說明', '是否必填', '備註']
OPTIONAL_COLUMNS = ['來源']  # 可選額外欄位

def validate_sheet_columns(df, sheet_name):
    """
    驗證工作表是否包含必要欄位
    """
    missing_columns = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    
    if missing_columns:
        logger.warning(f"工作表 '{sheet_name}' 缺少必要欄位: {missing_columns}")
        return False
    
    return True

def process_sheet(sheet_name, df):
    """
    處理單一工作表，轉換為 JSON 格式
    """
    try:
        # 檢查是否為空工作表
        if df.empty or df.dropna(how='all').empty:
            logger.info(f"跳過空工作表: {sheet_name}")
            return None
        
        # 移除完全空白的行
        df = df.dropna(how='all')
        
        # 驗證欄位
        if not validate_sheet_columns(df, sheet_name):
            logger.error(f"工作表 '{sheet_name}' 欄位驗證失敗，跳過處理")
            return None
        
        # 填充空值
        df = df.fillna('')
        
        mapping = {}
        processed_count = 0
        
        for index, row in df.iterrows():
            try:
                # 取得英文欄位名稱作為鍵值
                key = str(row['欄位英文']).strip()
                
                # 跳過空的英文欄位名稱
                if not key:
                    continue
                
                # 建立基本欄位對應
                field_mapping = {
                    'order': str(row['order']).strip(),
                    'zh_name': str(row['欄位中文']).strip(),
                    'type': str(row['型態']).strip(),
                    'description': str(row['說明']).strip(),
                    'required': str(row['是否必填']).strip(),
                    'note': str(row['備註']).strip()
                }
                
                # 處理可選欄位
                for optional_col in OPTIONAL_COLUMNS:
                    if optional_col in df.columns:
                        field_mapping[optional_col.lower()] = str(row[optional_col]).strip()
                
                mapping[key] = field_mapping
                processed_count += 1
                
            except Exception as e:
                logger.warning(f"處理工作表 '{sheet_name}' 第 {index+1} 行時發生錯誤: {e}")
                continue
        
        logger.info(f"工作表 '{sheet_name}' 處理完成，共處理 {processed_count} 個欄位")
        return mapping
        
    except Exception as e:
        logger.error(f"處理工作表 '{sheet_name}' 時發生錯誤: {e}")
        return None

def main():
    """
    讀取 mapping.xlsx 內所有分頁，逐一轉換成 json 欄位定義檔
    """
    logger.info("=== Excel to Mapping JSON 轉換開始 ===")
    
    # 檢查檔案是否存在
    if not MAPPING_XLSX.exists():
        logger.error(f"找不到檔案: {MAPPING_XLSX}")
        return
    
    try:
        # 讀取所有分頁到 dict
        logger.info(f"讀取檔案: {MAPPING_XLSX}")
        df_dict = pd.read_excel(MAPPING_XLSX, dtype=str, sheet_name=None)
        logger.info(f"發現 {len(df_dict)} 個工作表: {list(df_dict.keys())}")
        
        success_count = 0
        skip_count = 0
        error_count = 0
        
        for sheet_name, df in df_dict.items():
            logger.info(f"\n處理工作表: {sheet_name}")
            
            # 處理工作表
            mapping = process_sheet(sheet_name, df)
            
            if mapping is None:
                skip_count += 1
                continue
            
            if not mapping:
                logger.warning(f"工作表 '{sheet_name}' 沒有有效的欄位定義")
                skip_count += 1
                continue
            
            # 輸出 JSON 檔案
            try:
                output_file = Path(str(OUTPUT_TEMPLATE).format(sheet_name.lower()))
                
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(mapping, f, ensure_ascii=False, indent=2)
                
                logger.info(f"✅ 工作表 '{sheet_name}' → {output_file.name} (共 {len(mapping)} 個欄位)")
                success_count += 1
                
            except Exception as e:
                logger.error(f"儲存工作表 '{sheet_name}' 時發生錯誤: {e}")
                error_count += 1
        
        # 輸出摘要
        logger.info(f"\n=== 轉換完成摘要 ===")
        logger.info(f"成功處理: {success_count} 個工作表")
        logger.info(f"跳過處理: {skip_count} 個工作表")
        logger.info(f"處理失敗: {error_count} 個工作表")
        
        if success_count > 0:
            logger.info("✅ 所有有效工作表轉換完成！")
        else:
            logger.warning("⚠️ 沒有成功轉換任何工作表")
            
    except Exception as e:
        logger.error(f"讀取 Excel 檔案時發生錯誤: {e}")

if __name__ == "__main__":
    main()