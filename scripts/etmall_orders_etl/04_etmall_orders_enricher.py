# scripts/etmall_orders_etl/04_etmall_orders_enricher.py
"""
東森購物訂單資料增強腳本

功能：
- 讀取由 03_etmall_orders_deduplicator.py 產生的最新 CSV 檔案
- 載入 config/A02_Shops_Master.json
- 根據 'platform' 欄位將 'shop_id', 'shop_name', 'shop_business_model', 'location', 'department', 'manager' 欄位
  新增到訂單資料中
- 將最終的增強資料輸出到 temp/etmall/

使用方式：
直接執行此腳本
"""

import pandas as pd
from pathlib import Path
import sys
import logging
from datetime import datetime
import json
from typing import Dict, Any, List

def setup_logging(project_root: Path):
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    for log_file in log_dir.glob('etmall_orders_enricher_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")
            
    log_filename = f'etmall_orders_enricher_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, 'w', 'utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_json_config(file_path: Path) -> list:
    """
    載入 JSON 配置檔案
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"錯誤：找不到配置檔案 {file_path}")
        return []
    except json.JSONDecodeError as e:
        logging.error(f"錯誤：解析 JSON 檔案失敗 {file_path} - {e}")
        return []
    except Exception as e:
        logging.exception(f"錯誤：載入配置檔案時發生未知錯誤 {file_path}")
        return []

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)

    # 設定輸入與輸出目錄
    input_dir = project_root / 'temp' / 'etmall'
    output_dir = project_root / 'temp' / 'etmall'  # 輸出目錄與輸入目錄相同
    shop_master_file = project_root / 'config' / 'A02_Shops_Master.json'

    # 檢查目錄和配置檔案是否存在
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f'錯誤：找不到輸入目錄 {input_dir}')
        sys.exit(1)
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    if not shop_master_file.exists():
        logging.error(f'錯誤：找不到店家主檔 {shop_master_file}')
        sys.exit(1)

    # 清除舊的增強檔案
    logging.info(f'清除舊的增強檔案...')
    for old_file in output_dir.glob('04_etmall_orders_enriched_*.csv'):
        try:
            old_file.unlink()
            logging.info(f'已刪除舊檔案：{old_file.name}')
        except OSError as e:
            logging.error(f"錯誤: 無法刪除舊增強檔案 {old_file.name} - {e}")
            
    logging.info(f'讀取輸入目錄：{input_dir}')
    logging.info(f'輸出目錄：{output_dir}')

    # 載入店家主檔配置
    shop_master_data = load_json_config(shop_master_file)
    if not shop_master_data:
        logging.error('無法載入店家主檔，停止執行')
        sys.exit(1)
    
    # 提取 shops 列表
    if 'shops' in shop_master_data:
        shop_master_list = shop_master_data['shops']
        logging.info(f'從店家主檔中載入了 {len(shop_master_list)} 個店家資訊')
    else:
        # 如果沒有 shops 鍵，假設整個文件就是店家列表
        shop_master_list = shop_master_data
        logging.info(f'載入了 {len(shop_master_list)} 個店家資訊')

    # 尋找最新的 etmall 合併檔案
    logging.info(f'\n=== 尋找最新的合併檔案 ===')
    merged_files = sorted(list(input_dir.glob('03_etmall_orders_merged_*.csv')), reverse=True)
    
    if not merged_files:
        logging.error(f'在 {input_dir} 目錄下沒有找到任何合併後的檔案，請先執行 03_etmall_orders_deduplicator.py')
        sys.exit(1)
    
    latest_file = merged_files[0]
    logging.info(f'找到最新合併檔案：{latest_file.name}')

    try:
        df_orders = pd.read_csv(latest_file, dtype=str)
        logging.info(f'已讀取合併資料，總共 {len(df_orders)} 筆')
    except Exception as e:
        logging.exception(f'錯誤：讀取合併檔案失敗：{latest_file.name}')
        sys.exit(1)

    logging.info(f'\n=== 開始新增欄位 ===')

    # 將店家主檔轉換為 DataFrame，以利合併
    df_shops = pd.DataFrame(shop_master_list)
    
    # 檢查訂單資料中是否有 platform 欄位
    if 'platform' not in df_orders.columns:
        logging.warning("訂單資料中沒有 'platform' 欄位，將新增預設值 'etmall'")
        df_orders['platform'] = 'etmall'
    
    # 獲取訂單資料中的唯一 platform 值
    unique_platforms = df_orders['platform'].unique()
    logging.info(f"訂單資料中的平台：{unique_platforms}")
    
    # 為每個平台尋找對應的店家資訊
    for platform in unique_platforms:
        logging.info(f"處理平台：{platform}")
        
        # 根據平台名稱尋找對應的店家資訊
        # 這裡可以根據實際需求調整匹配邏輯
        if platform == 'etmall':
            shop_filter = df_shops['shop_name'] == '東森購物'
        elif platform == 'momo':
            shop_filter = df_shops['shop_name'] == 'MOMO購物中心'
        elif platform == 'pchome':
            shop_filter = df_shops['shop_name'] == 'PC購物中心'
        elif platform == 'yahoo':
            shop_filter = df_shops['shop_name'] == 'Yahoo購物中心'
        else:
            # 對於未知平台，嘗試模糊匹配
            shop_filter = df_shops['shop_name'].str.contains(platform, case=False, na=False)
        
        platform_shop_info = df_shops[shop_filter]
        
        if not platform_shop_info.empty:
            # 取得該平台的第一筆店家資訊
            shop_info = platform_shop_info.iloc[0]
            
            # 為該平台的所有訂單新增店家相關欄位
            platform_mask = df_orders['platform'] == platform
            
            df_orders.loc[platform_mask, 'shop_id'] = shop_info['shop_id']
            df_orders.loc[platform_mask, 'shop_name'] = shop_info['shop_name']
            df_orders.loc[platform_mask, 'shop_business_model'] = shop_info['shop_business_model']
            df_orders.loc[platform_mask, 'location'] = shop_info['location']
            df_orders.loc[platform_mask, 'department'] = shop_info['department']
            df_orders.loc[platform_mask, 'manager'] = shop_info['manager']
            
            logging.info(f"已為平台 '{platform}' 新增店家資訊：{shop_info['shop_name']} (ID: {shop_info['shop_id']})")
        else:
            logging.warning(f"在店家主檔中未找到平台 '{platform}' 的對應店家資訊，相關欄位將保持空白")
            # 為該平台的所有訂單新增空白欄位
            platform_mask = df_orders['platform'] == platform
            
            df_orders.loc[platform_mask, 'shop_id'] = ''
            df_orders.loc[platform_mask, 'shop_name'] = ''
            df_orders.loc[platform_mask, 'shop_business_model'] = ''
            df_orders.loc[platform_mask, 'location'] = ''
            df_orders.loc[platform_mask, 'department'] = ''
            df_orders.loc[platform_mask, 'manager'] = ''

    # 儲存最終的增強檔案
    output_filename = f'04_etmall_orders_enriched_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = output_dir / output_filename
    df_orders.to_csv(output_path, index=False, encoding='utf-8-sig')

    logging.info(f'\n=== 增強完成 ===')
    logging.info(f'最終增強後檔案位置：{output_path}')
    logging.info(f'最終資料筆數：{len(df_orders)}')

if __name__ == '__main__':
    main()
