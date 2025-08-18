#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo 訂單資料豐富腳本
負責載入 config/A02_Shops_Master.json 的商店主檔資料
將 shop_name="Yahoo購物中心" 的相關資訊映射到訂單資料中

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import json
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
    log_filename = LOGS_DIR / f"yahoo_enricher_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def load_shops_master() -> Dict:
    """載入商店主檔配置"""
    try:
        config_file = CONFIG_DIR / "A02_Shops_Master.json"
        if not config_file.exists():
            raise FileNotFoundError(f"找不到商店主檔配置檔案：{config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            shops_master = json.load(f)
        
        logging.info(f"成功載入商店主檔配置，共 {len(shops_master)} 筆資料")
        return shops_master
        
    except Exception as e:
        logging.error(f"載入商店主檔配置失敗：{e}")
        raise

def find_yahoo_shop_info(shops_master: Dict) -> Optional[Dict]:
    """尋找 Yahoo 購物中心的商店資訊（硬編碼映射）"""
    try:
        # 硬編碼映射：platform='yahoo' 對應 shop_name='Yahoo購物中心'
        yahoo_platform = 'yahoo'
        yahoo_shop_name = 'Yahoo購物中心'
        
        logging.info(f"使用硬編碼映射：platform='{yahoo_platform}' -> shop_name='{yahoo_shop_name}'")
        
        # 檢查資料結構
        if 'shops' not in shops_master:
            logging.error("商店主檔格式錯誤：缺少 'shops' 陣列")
            return None
        
        shops_array = shops_master['shops']
        logging.info(f"找到 {len(shops_array)} 個商店記錄")
        
        for shop in shops_array:
            if shop.get('shop_name') == yahoo_shop_name:
                logging.info(f"找到 Yahoo 購物中心商店資訊：{shop}")
                return shop
        
        logging.warning(f"在商店主檔中找不到 shop_name='{yahoo_shop_name}' 的資訊")
        return None
        
    except Exception as e:
        logging.error(f"尋找 Yahoo 商店資訊時發生錯誤：{e}")
        return None

def find_latest_orders_file() -> Optional[Path]:
    """尋找 temp/Yahoo 下最新的 yahoo_orders_merged_*.csv 檔案"""
    try:
        if not TEMP_DIR.exists():
            logging.error(f"目錄不存在：{TEMP_DIR}")
            return None
        
        # 尋找所有 yahoo_orders_merged_*.csv 檔案
        pattern = "yahoo_orders_merged_*.csv"
        order_files = list(TEMP_DIR.glob(pattern))
        
        if not order_files:
            logging.warning(f"在 {TEMP_DIR} 中找不到 {pattern} 檔案")
            return None
        
        # 按檔案修改時間排序，取最新的
        latest_file = max(order_files, key=lambda x: x.stat().st_mtime)
        logging.info(f"找到最新的訂單檔案：{latest_file.name}")
        
        return latest_file
        
    except Exception as e:
        logging.error(f"尋找最新訂單檔案時發生錯誤：{e}")
        return None

def enrich_orders_data(df: pd.DataFrame, shop_info: Dict) -> pd.DataFrame:
    """豐富訂單資料，添加商店主檔資訊（基於硬編碼映射：platform='yahoo' -> shop_name='Yahoo購物中心'）"""
    try:
        logging.info(f"開始豐富訂單資料，原始資料共 {len(df)} 行")
        
        # 創建副本避免修改原始資料
        enriched_df = df.copy()
        
        # 添加商店主檔欄位
        shop_fields = [
            'shop_id', 'shop_account', 'shop_status', 
            'shop_business_model', 'location', 'department', 'manager'
        ]
        
        for field in shop_fields:
            if field in shop_info:
                enriched_df[field] = shop_info[field]
                logging.info(f"添加欄位 {field}：{shop_info[field]}")
            else:
                enriched_df[field] = ''
                logging.warning(f"商店主檔中缺少欄位：{field}")
        
        # 確保 platform 欄位為 'yahoo'
        if 'platform' in enriched_df.columns:
            enriched_df['platform'] = 'yahoo'
            logging.info("更新 platform 欄位為 'yahoo'")
        else:
            enriched_df['platform'] = 'yahoo'
            logging.info("添加 platform 欄位：'yahoo'")
        
        logging.info(f"資料豐富完成，最終資料共 {len(enriched_df)} 行，{len(enriched_df.columns)} 個欄位")
        logging.info(f"最終欄位：{list(enriched_df.columns)}")
        
        return enriched_df
        
    except Exception as e:
        logging.error(f"豐富訂單資料時發生錯誤：{e}")
        import traceback
        logging.error(f"錯誤詳情：{traceback.format_exc()}")
        return df

def save_enriched_file(df: pd.DataFrame, output_dir: Path, logger: logging.Logger) -> bool:
    """儲存豐富後的檔案"""
    try:
        # 生成輸出檔案名稱
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"yahoo_orders_enriched_{timestamp}.csv"
        output_path = output_dir / output_filename
        
        # 儲存檔案
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"豐富後的檔案已儲存至：{output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"儲存豐富檔案時發生錯誤：{e}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("=== Yahoo 訂單資料豐富作業開始 ===")
    
    try:
        # 1. 載入商店主檔配置
        logger.info("步驟 1：載入商店主檔配置")
        shops_master = load_shops_master()
        
        # 2. 尋找 Yahoo 購物中心商店資訊
        logger.info("步驟 2：尋找 Yahoo 購物中心商店資訊")
        yahoo_shop_info = find_yahoo_shop_info(shops_master)
        
        if not yahoo_shop_info:
            logger.error("無法找到 Yahoo 購物中心商店資訊，作業終止")
            return 1
        
        # 3. 尋找最新的訂單檔案
        logger.info("步驟 3：尋找最新的訂單檔案")
        latest_orders_file = find_latest_orders_file()
        
        if not latest_orders_file:
            logger.error("無法找到最新的訂單檔案，作業終止")
            return 1
        
        # 4. 讀取訂單檔案
        logger.info("步驟 4：讀取訂單檔案")
        try:
            # 嘗試不同的編碼
            encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
            df = None
            
            for encoding in encodings:
                try:
                    df = pd.read_csv(latest_orders_file, dtype=str, encoding=encoding)
                    logger.info(f"使用編碼 {encoding} 成功讀取檔案")
                    break
                except UnicodeDecodeError:
                    continue
                except Exception as e:
                    logger.warning(f"編碼 {encoding} 讀取失敗：{e}")
                    continue
            
            if df is None:
                logger.error("無法讀取訂單檔案")
                return 1
                
        except Exception as e:
            logger.error(f"讀取訂單檔案時發生錯誤：{e}")
            return 1
        
        logger.info(f"成功讀取訂單檔案，共 {len(df)} 行，{len(df.columns)} 個欄位")
        logger.info(f"原始欄位：{list(df.columns)}")
        
        # 5. 豐富訂單資料
        logger.info("步驟 5：豐富訂單資料")
        enriched_df = enrich_orders_data(df, yahoo_shop_info)
        
        # 6. 儲存豐富後的檔案
        logger.info("步驟 6：儲存豐富後的檔案")
        if save_enriched_file(enriched_df, OUTPUT_DIR, logger):
            logger.info("✅ 資料豐富作業完成！")
        else:
            logger.error("❌ 儲存檔案失敗")
            return 1
        
        # 7. 輸出結果摘要
        logger.info("=" * 50)
        logger.info("處理結果摘要")
        logger.info("=" * 50)
        logger.info(f"輸入檔案：{latest_orders_file.name}")
        logger.info(f"原始資料：{len(df)} 行，{len(df.columns)} 個欄位")
        logger.info(f"豐富後資料：{len(enriched_df)} 行，{len(enriched_df.columns)} 個欄位")
        logger.info(f"新增欄位：{len(enriched_df.columns) - len(df.columns)} 個")
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
