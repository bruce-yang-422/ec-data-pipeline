#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo 訂單產品資料豐富腳本
負責載入 config/products.yaml 的產品主檔資料
以條碼為key對應 supplier_product_code，增加產品相關資訊

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import yaml
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

# 設定路徑
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
TEMP_DIR = PROJECT_ROOT / "temp" / "Yahoo"
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"
OUTPUT_DIR = PROJECT_ROOT / "temp" / "Yahoo"

# 確保目錄存在
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# 設定日誌
def setup_logging():
    """設定日誌"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = LOGS_DIR / f"yahoo_product_enricher_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def load_products_master() -> Dict:
    """載入產品主檔配置"""
    try:
        config_file = CONFIG_DIR / "products.yaml"
        if not config_file.exists():
            raise FileNotFoundError(f"找不到產品主檔配置檔案：{config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            products_master = yaml.safe_load(f)
        
        logging.info(f"成功載入產品主檔配置，共 {len(products_master)} 筆資料")
        return products_master
        
    except Exception as e:
        logging.error(f"載入產品主檔配置失敗：{e}")
        raise

def find_latest_enriched_file() -> Optional[Path]:
    """尋找 temp/Yahoo 下最新的 yahoo_orders_enriched_*.csv 檔案"""
    try:
        if not TEMP_DIR.exists():
            logging.error(f"目錄不存在：{TEMP_DIR}")
            return None
        
        # 尋找所有 yahoo_orders_enriched_*.csv 檔案
        pattern = "yahoo_orders_enriched_*.csv"
        enriched_files = list(TEMP_DIR.glob(pattern))
        
        if not enriched_files:
            logging.warning(f"在 {TEMP_DIR} 中找不到 {pattern} 檔案")
            return None
        
        # 按檔案修改時間排序，取最新的
        latest_file = max(enriched_files, key=lambda x: x.stat().st_mtime)
        logging.info(f"找到最新的豐富後檔案：{latest_file.name}")
        
        return latest_file
        
    except Exception as e:
        logging.error(f"尋找最新豐富後檔案時發生錯誤：{e}")
        return None

def enrich_orders_with_products(df: pd.DataFrame, products_master: Dict) -> pd.DataFrame:
    """豐富訂單資料，添加產品主檔資訊"""
    try:
        logging.info(f"開始豐富產品資料，原始資料共 {len(df)} 行")
        
        # 創建副本避免修改原始資料
        enriched_df = df.copy()
        
        # 定義要添加的產品欄位
        product_fields = [
            'category_level_1', 'category_level_2', 'brand', 'series', 'pet_type',
            'product_name', 'sku', 'tags', 'spec', 'unit', 'weight_g', 'package_size',
            'barcode', 'msrp', 'price', 'supplier_price', 'list_price', 'cost',
            'stock_status', 'supplier_code', 'supplier', 'supplier_ref'
        ]
        
        # 初始化新欄位為空值（但保留原有的 product_name）
        for field in product_fields:
            if field == 'product_name':
                # 保留原始品名，不覆蓋
                if field not in enriched_df.columns:
                    enriched_df[field] = ''
            else:
                enriched_df[field] = ''
        
        # 統計匹配成功的記錄數
        matched_count = 0
        
        # 遍歷每一行訂單資料
        for idx, row in enriched_df.iterrows():
            supplier_product_code = row.get('supplier_product_code', '')
            
            if supplier_product_code and supplier_product_code in products_master:
                # 找到對應的產品資訊
                product_info = products_master[supplier_product_code]
                matched_count += 1
                
                # 填充產品欄位
                for field in product_fields:
                    if field in product_info:
                        # 對於 product_name，只有在產品主檔中有更好的名稱時才覆蓋
                        if field == 'product_name':
                            master_name = product_info[field]
                            current_name = enriched_df.at[idx, field]
                            # 如果產品主檔的名稱更完整或原始名稱為空，則使用產品主檔的名稱
                            if master_name and (not current_name or len(master_name) > len(str(current_name))):
                                enriched_df.at[idx, field] = master_name
                        else:
                            enriched_df.at[idx, field] = product_info[field]
                    else:
                        # 對於 product_name 以外的欄位，如果沒有資料就設為空字串
                        if field != 'product_name':
                            enriched_df.at[idx, field] = ''
                
                if idx < 5:  # 只記錄前5筆的詳細資訊
                    logging.info(f"行 {idx}: 條碼 {supplier_product_code} 匹配成功")
            else:
                if idx < 5:  # 只記錄前5筆的詳細資訊
                    logging.warning(f"行 {idx}: 條碼 {supplier_product_code} 在產品主檔中找不到")
        
        logging.info(f"產品資料豐富完成，共 {matched_count} 筆資料匹配成功")
        logging.info(f"最終資料共 {len(enriched_df)} 行，{len(enriched_df.columns)} 個欄位")
        logging.info(f"最終欄位：{list(enriched_df.columns)}")
        
        return enriched_df
        
    except Exception as e:
        logging.error(f"豐富產品資料時發生錯誤：{e}")
        import traceback
        logging.error(f"錯誤詳情：{traceback.format_exc()}")
        return df

def save_product_enriched_file(df: pd.DataFrame, output_dir: Path, logger: logging.Logger) -> bool:
    """儲存產品豐富後的檔案"""
    try:
        # 生成輸出檔案名稱
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"yahoo_orders_product_enriched_{timestamp}.csv"
        output_path = output_dir / output_filename
        
        # 儲存檔案
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"產品豐富後的檔案已儲存至：{output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"儲存產品豐富檔案時發生錯誤：{e}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("=== Yahoo 訂單產品資料豐富作業開始 ===")
    
    try:
        # 1. 載入產品主檔配置
        logger.info("步驟 1：載入產品主檔配置")
        products_master = load_products_master()
        
        # 2. 尋找最新的豐富後檔案
        logger.info("步驟 2：尋找最新的豐富後檔案")
        latest_enriched_file = find_latest_enriched_file()
        
        if not latest_enriched_file:
            logger.error("無法找到最新的豐富後檔案，作業終止")
            return 1
        
        # 3. 讀取豐富後檔案
        logger.info("步驟 3：讀取豐富後檔案")
        try:
            # 嘗試不同的編碼
            encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(latest_enriched_file, dtype=str, encoding=encoding)
                    logger.info(f"使用編碼 {encoding} 成功讀取檔案")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"編碼 {encoding} 讀取失敗：{e}")
                    continue
            
            if df is None:
                logger.error("無法讀取豐富後檔案")
                return 1
                
        except Exception as e:
            logger.error(f"讀取豐富後檔案時發生錯誤：{e}")
            return 1
        
        logger.info(f"成功讀取豐富後檔案，共 {len(df)} 行，{len(df.columns)} 個欄位")
        logger.info(f"原始欄位：{list(df.columns)}")
        
        # 4. 豐富產品資料
        logger.info("步驟 4：豐富產品資料")
        product_enriched_df = enrich_orders_with_products(df, products_master)
        
        # 5. 儲存產品豐富後的檔案
        logger.info("步驟 5：儲存產品豐富後的檔案")
        if save_product_enriched_file(product_enriched_df, OUTPUT_DIR, logger):
            logger.info("✅ 產品資料豐富作業完成！")
        else:
            logger.error("❌ 儲存檔案失敗")
            return 1
        
        # 6. 輸出結果摘要
        logger.info("=" * 50)
        logger.info("處理結果摘要")
        logger.info("=" * 50)
        logger.info(f"輸入檔案：{latest_enriched_file.name}")
        logger.info(f"原始資料：{len(df)} 行，{len(df.columns)} 個欄位")
        logger.info(f"產品豐富後資料：{len(product_enriched_df)} 行，{len(product_enriched_df.columns)} 個欄位")
        logger.info(f"新增欄位：{len(product_enriched_df.columns) - len(df.columns)} 個")
        logger.info(f"輸出目錄：{OUTPUT_DIR}")
        logger.info("=" * 50)
        
        return 0
        
    except Exception as e:
        logger.error(f"作業執行時發生未預期的錯誤：{e}")
        import traceback
        logger.error(f"錯誤詳情：{traceback.format_exc()}")
        return 1

if __name__ == "__main__":
    exit(main())
