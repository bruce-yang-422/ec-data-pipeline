#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
東森購物銷售報表合併腳本
合併 temp/etmall/Sales_Report 下所有 CSV 檔案，並以 order_line_uid 為 key 去除重複
"""

import os
import sys
import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# 設定專案根目錄
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(project_root / 'logs' / 'etmall_sales_report_merger.log', encoding='utf-8'),
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

def merge_sales_report_files(csv_files: list, output_dir: Path) -> str:
    """
    合併銷售報表檔案並去除重複
    
    Args:
        csv_files: CSV 檔案路徑列表
        output_dir: 輸出目錄
        
    Returns:
        str: 輸出檔案路徑
    """
    if not csv_files:
        logging.warning("沒有找到任何 CSV 檔案")
        return None
    
    # 建立輸出目錄
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # 合併所有 CSV 檔案
    all_dataframes = []
    
    for file_path in csv_files:
        try:
            logging.info(f"讀取檔案：{file_path.name}")
            
            # 讀取 CSV 檔案
            df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=str)
            
            # 記錄檔案資訊
            logging.info(f"  - 檔案：{file_path.name}")
            logging.info(f"  - 資料筆數：{len(df)} 筆")
            logging.info(f"  - 欄位數量：{len(df.columns)} 個")
            
            # 檢查是否有 order_line_uid 欄位
            if 'order_line_uid' not in df.columns:
                logging.warning(f"檔案 {file_path.name} 缺少 order_line_uid 欄位，跳過")
                continue
            
            all_dataframes.append(df)
            
        except Exception as e:
            logging.error(f"讀取檔案失敗：{file_path.name} - {str(e)}")
            continue
    
    if not all_dataframes:
        logging.error("沒有成功讀取任何檔案")
        return None
    
    # 合併所有 DataFrame
    logging.info("開始合併所有資料...")
    merged_df = pd.concat(all_dataframes, ignore_index=True)
    
    logging.info(f"合併前總資料筆數：{len(merged_df)} 筆")
    
    # 檢查重複的 order_line_uid
    duplicate_count = merged_df['order_line_uid'].duplicated().sum()
    logging.info(f"發現重複的 order_line_uid：{duplicate_count} 筆")
    
    if duplicate_count > 0:
        # 顯示重複的 order_line_uid 範例
        duplicates = merged_df[merged_df['order_line_uid'].duplicated(keep=False)]['order_line_uid'].unique()[:10]
        logging.info(f"重複的 order_line_uid 範例：{list(duplicates)}")
        
        # 去除重複，保留最後一筆（新蓋舊）
        merged_df = merged_df.drop_duplicates(subset=['order_line_uid'], keep='last')
        logging.info(f"去除重複後資料筆數：{len(merged_df)} 筆")
        logging.info(f"移除重複資料：{duplicate_count} 筆")
    
    # 處理 item_no 格式（個位數前面補0）
    if 'item_no' in merged_df.columns:
        merged_df['item_no'] = merged_df['item_no'].astype(str).str.zfill(2)
        logging.info("已處理 item_no 格式（個位數前面補0）")
    
    # 按照 order_sn 和 item_no 排序
    if 'order_sn' in merged_df.columns and 'item_no' in merged_df.columns:
        # 轉換為數值型態進行排序
        merged_df['order_sn_numeric'] = pd.to_numeric(merged_df['order_sn'], errors='coerce')
        merged_df['item_no_numeric'] = pd.to_numeric(merged_df['item_no'], errors='coerce')
        
        # 排序
        merged_df = merged_df.sort_values(['order_sn_numeric', 'item_no_numeric'], ascending=[True, True])
        
        # 移除臨時欄位
        merged_df = merged_df.drop(['order_sn_numeric', 'item_no_numeric'], axis=1)
        
        logging.info("已按照 order_sn 和 item_no 排序（由小到大）")
    
    # 生成輸出檔案名稱
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f"etmall_sales_report_merged_{timestamp}.csv"
    output_path = output_dir / output_filename
    
    # 儲存合併後的資料
    try:
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"✅ 合併完成：{output_filename}")
        logging.info(f"   輸出位置：{output_path}")
        logging.info(f"   最終資料筆數：{len(merged_df)} 筆")
        logging.info(f"   欄位數量：{len(merged_df.columns)} 個")
        
        return str(output_path)
        
    except Exception as e:
        logging.error(f"儲存檔案失敗：{str(e)}")
        return None

def main():
    """主函數"""
    logging.info("=" * 50)
    logging.info("開始執行東森購物銷售報表合併腳本")
    logging.info("=" * 50)
    
    # 設定路徑
    sales_report_dir = project_root / "temp" / "etmall" / "Sales_Report"
    output_dir = project_root / "temp" / "etmall"
    
    logging.info(f"專案根目錄：{project_root}")
    logging.info(f"銷售報表目錄：{sales_report_dir}")
    logging.info(f"輸出目錄：{output_dir}")
    
    # 尋找所有 CSV 檔案
    csv_files = find_sales_report_files(sales_report_dir)
    
    if not csv_files:
        logging.error("沒有找到任何 CSV 檔案，程式結束")
        return
    
    logging.info(f"找到 {len(csv_files)} 個 CSV 檔案需要合併")
    
    # 合併檔案
    output_path = merge_sales_report_files(csv_files, output_dir)
    
    if output_path:
        logging.info("=" * 50)
        logging.info("📊 合併結果總結：")
        logging.info(f"   - 處理檔案：{len(csv_files)} 個")
        logging.info(f"   - 輸出檔案：{Path(output_path).name}")
        logging.info(f"   - 輸出位置：{output_path}")
        logging.info("✅ 銷售報表合併完成！")
        logging.info("=" * 50)
    else:
        logging.error("❌ 合併失敗！")

if __name__ == "__main__":
    main()
