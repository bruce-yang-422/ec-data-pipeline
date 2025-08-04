# scripts/etmall_orders_etl/04_etmall_orders_enricher.py
"""
東森購物訂單資料增強腳本

功能：
- 讀取由 etmall_merger.py 產生的合併後 CSV 檔案
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
    shop_master_list = load_json_config(shop_master_file)
    if not shop_master_list:
        logging.error('無法載入店家主檔，停止執行')
        sys.exit(1)

    # 尋找最新的 etmall 合併後檔案
    logging.info(f'\n=== 尋找最新的合併檔案 ===')
    merged_files = sorted(list(input_dir.glob('etmall_orders_merged_*.csv')), reverse=True)

    if not merged_files:
        logging.warning(f'在 {input_dir} 目錄下沒有找到任何合併後的檔案')
        return

    latest_merged_file = merged_files[0]
    logging.info(f'找到最新檔案：{latest_merged_file.name}')

    try:
        df_orders = pd.read_csv(latest_merged_file, dtype=str)
        logging.info(f'已讀取訂單資料，總共 {len(df_orders)} 筆')
    except Exception as e:
        logging.exception(f'錯誤：讀取訂單檔案失敗：{latest_merged_file.name}')
        sys.exit(1)

    logging.info(f'\n=== 開始新增欄位 ===')

    # 將店家主檔轉換為 DataFrame，以利合併
    df_shops = pd.DataFrame(shop_master_list)
    df_shops = df_shops[['shop_id', 'shop_name', 'shop_business_model', 'location', 'department', 'manager']]

    # 執行合併 (Join)
    etmall_shop_info = df_shops[df_shops['shop_name'] == '東森購物'].iloc[0]

    if not etmall_shop_info.empty:
        df_orders['shop_id'] = etmall_shop_info['shop_id']
        df_orders['shop_name'] = etmall_shop_info['shop_name']
        df_orders['shop_business_model'] = etmall_shop_info['shop_business_model']
        df_orders['location'] = etmall_shop_info['location']
        df_orders['department'] = etmall_shop_info['department']
        df_orders['manager'] = etmall_shop_info['manager']
        logging.info('已成功新增店家相關欄位')
    else:
        logging.warning("在店家主檔中未找到 '東森購物' 的資料，無法新增欄位")

    # 儲存最終的增強檔案
    output_filename = f'04_etmall_orders_enriched_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
    output_path = output_dir / output_filename
    df_orders.to_csv(output_path, index=False, encoding='utf-8-sig')

    logging.info(f'\n=== 增強完成 ===')
    logging.info(f'最終增強後檔案位置：{output_path}')
    logging.info(f'最終資料筆數：{len(df_orders)}')

if __name__ == '__main__':
    main()
