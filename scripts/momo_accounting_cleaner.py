# scripts/momo_accounting_cleaner.py
# -*- coding: utf-8 -*-
"""
MOMO 帳務對帳訂單清理腳本 (C1105 系列)

功能：
- 批次讀取 temp/momo/C1105_*.csv 檔案
- 按 c1105_momo_fields_mapping.json 定義調整欄位與順序
- 輸出到 data_processed/merged/momo_accounting_orders_cleaned.csv

使用：python scripts/momo_accounting_cleaner.py
"""

import pandas as pd
import json
import os
import sys
import logging
from datetime import datetime
from glob import glob
from pathlib import Path

class MomoAccountingCleaner:
    def __init__(self):
        # 路徑設定
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parent
        
        # 檔案路徑
        self.mapping_path = self.project_root / "config" / "c1105_momo_fields_mapping.json"
        self.source_dir = self.project_root / "temp" / "momo"
        self.output_dir = self.project_root / "data_processed" / "merged"
        self.output_path = self.output_dir / "momo_accounting_orders_cleaned.csv"
        self.logs_dir = self.project_root / "logs"
        
        # 確保目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
    def setup_logging(self):
        """設定日誌系統"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"momo_accounting_cleaner_{timestamp}.log"
        log_path = self.logs_dir / log_filename
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_path, encoding='utf-8'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== MOMO 帳務對帳訂單清理開始 ===")
        
    def get_mapping(self):
        """讀取 C1105 mapping 設定並根據 'order' 欄位排序"""
        try:
            with open(self.mapping_path, "r", encoding="utf-8") as f:
                mapping = json.load(f)
            
            # 根據 mapping JSON 中的 "order" 值對欄位進行排序
            columns = sorted(mapping.keys(), key=lambda k: int(mapping[k]["order"]))
            
            self.logger.info(f"載入 mapping 配置：{len(mapping)} 個欄位")
            return mapping, columns
            
        except Exception as e:
            self.logger.error(f"載入 mapping 失敗：{e}")
            raise
    
    def read_csv_files(self, mapping):
        """讀取 C1105 CSV 檔案"""
        # 尋找 C1105 開頭的檔案
        c1105_files = glob(str(self.source_dir / "C1105_*.csv"))
        
        if not c1105_files:
            self.logger.warning(f"在 {self.source_dir} 目錄下沒有找到 C1105 CSV 檔案")
            return pd.DataFrame()
        
        self.logger.info(f"找到 {len(c1105_files)} 個 C1105 檔案")
        
        # 建立中文到英文的欄位對應
        zh_to_en = {v["zh_name"]: k for k, v in mapping.items()}
        
        dfs = []
        for file_path in c1105_files:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding='utf-8-sig').fillna("")
                
                # 重新命名欄位
                df = df.rename(columns=zh_to_en)
                
                # 過濾空的訂單編號
                if 'order_sn' in df.columns:
                    df = df[df['order_sn'].str.strip() != ""]
                
                # 清理字串欄位
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                        # 清理數值欄位的 .0 後綴
                        if col in ['product_sku_main', 'quantity']:
                            df[col] = df[col].str.replace(r'\.0$', '', regex=True)
                
                # 標記資料來源
                file_name = Path(file_path).name
                df['data_source'] = 'C1105'
                
                dfs.append(df)
                self.logger.info(f"讀取成功：{file_name} ({len(df)} 筆)")
                
            except Exception as e:
                self.logger.error(f"讀取失敗：{file_path} - {e}")
        
        if not dfs:
            return pd.DataFrame()
        
        # 合併所有資料
        combined_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"總共讀取 {len(combined_df)} 筆原始資料")
        
        return combined_df
    
    def process_data(self, df, mapping, columns):
        """根據 mapping 處理資料"""
        self.logger.info("開始資料處理...")
        
        # 添加固定欄位
        df['platform'] = 'momo'
        df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 處理訂單日期 (C1105 有原始的訂單成立日)
        if 'order_date' in df.columns:
            # 標準化日期格式
            def standardize_date(date_str):
                if not isinstance(date_str, str) or not date_str.strip():
                    return ""
                try:
                    # 嘗試解析 YYYY/MM/DD 格式
                    if '/' in date_str:
                        parts = date_str.split('/')
                        if len(parts) == 3:
                            year, month, day = parts
                            return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
                    return date_str
                except:
                    return ""
            
            df['order_date'] = df['order_date'].apply(standardize_date)
        else:
            # 如果沒有訂單日期欄位，從訂單編號解析
            def parse_order_date(order_sn):
                if isinstance(order_sn, str) and len(order_sn) >= 6:
                    date_part = order_sn[:6]
                    if date_part.isdigit():
                        y, m, d = date_part[:2], date_part[2:4], date_part[4:6]
                        year = int(y) + 2000
                        return f"{year:04d}-{int(m):02d}-{int(d):02d}"
                return ""
            
            if 'order_sn' in df.columns:
                df['order_date'] = df['order_sn'].apply(parse_order_date)
        
        # 解析訂單編號組成 (保持 00X 格式)
        def parse_order_sn_components(order_sn):
            if isinstance(order_sn, str) and '-' in order_sn:
                parts = order_sn.split('-')
                if len(parts) == 4:
                    # 保持原始的 00X 格式
                    return pd.Series([parts[0], parts[1], parts[2], parts[3]])
                else:
                    return pd.Series([parts[0], '001', '001', '001'])
            return pd.Series([order_sn, '001', '001', '001'])

        if 'order_sn' in df.columns:
            df[['order_sn_main', 'order_line_number', 'order_sub_sequence', 'order_detail_sequence']] = \
                df['order_sn'].apply(parse_order_sn_components)

        # 判斷是否為異常單
        def is_abnormal(order_sn):
            if isinstance(order_sn, str) and len(order_sn) > 17:
                return not order_sn.endswith('001-001')
            return False
        
        if 'order_sn' in df.columns:
            df['is_abnormal_order'] = df['order_sn'].apply(is_abnormal)
        
        # 生成合併鍵
        if 'order_sn' in df.columns:
            df['key_for_merge'] = 'momo_' + df['order_sn'].astype(str)
        
        # 處理日期時間欄位
        datetime_fields = ['order_transfer_date', 'actual_shipping_date']
        for field in datetime_fields:
            if field in df.columns:
                def standardize_datetime(dt_str):
                    if not isinstance(dt_str, str) or not dt_str.strip():
                        return ""
                    try:
                        # 處理 YYYY/MM/DD 格式
                        if '/' in dt_str:
                            date_part = dt_str.split(' ')[0]  # 取日期部分
                            parts = date_part.split('/')
                            if len(parts) == 3:
                                year, month, day = parts
                                formatted_date = f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
                                if ' ' in dt_str:  # 有時間部分
                                    time_part = dt_str.split(' ')[1]
                                    return f"{formatted_date} {time_part}"
                                else:
                                    return formatted_date
                        return dt_str
                    except:
                        return ""
                
                df[field] = df[field].apply(standardize_datetime)
        
        # 確保所有欄位存在並設定正確的資料類型 (BigQuery 相容)
        for col in columns:
            if col not in df.columns:
                df[col] = ''
            
            if col in mapping:
                data_type = mapping[col].get('type', 'STRING')
                
                # BigQuery 相容的資料類型處理
                if data_type in ['INTEGER', 'INT64']:
                    # 數量等整數欄位，確保沒有小數點
                    if col == 'quantity':
                        # 特別處理 quantity，移除小數點並轉為整數
                        df[col] = df[col].astype(str).str.replace(r'\..*$', '', regex=True)
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                    else:
                        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')
                
                elif data_type in ['FLOAT', 'FLOAT64', 'NUMERIC']:
                    # BigQuery NUMERIC 類型，保留小數
                    df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
                
                elif data_type in ['BOOLEAN', 'BOOL']:
                    # BigQuery BOOLEAN 類型
                    df[col] = df[col].astype(str).str.lower().isin(['true', '1', 'yes', 'y'])
                
                elif data_type in ['DATE']:
                    # BigQuery DATE 類型 (YYYY-MM-DD)
                    # 保持字串格式，確保格式正確
                    pass
                
                elif data_type in ['DATETIME', 'TIMESTAMP']:
                    # BigQuery DATETIME/TIMESTAMP 類型
                    # 保持字串格式，確保格式正確
                    pass
                
                else:
                    # STRING 類型，確保為字串
                    df[col] = df[col].astype(str)
        
        # 按照指定順序排列欄位
        processed_df = df[columns]
        self.logger.info(f"資料處理完成，共 {len(processed_df)} 筆")
        
        return processed_df
    
    def save_data(self, df, columns):
        """儲存資料，處理合併與去重"""
        try:
            if self.output_path.exists():
                try:
                    old_df = pd.read_csv(self.output_path, dtype=str).fillna("")
                    combined = pd.concat([old_df, df], ignore_index=True)
                    combined = combined.drop_duplicates(subset=['key_for_merge'], keep='last')
                    self.logger.info("與現有資料合併完成")
                except pd.errors.EmptyDataError:
                    combined = df
                    self.logger.info("現有檔案為空，直接使用新資料")
            else:
                combined = df
                self.logger.info("建立新的輸出檔案")
            
            # 強制重新排序欄位
            combined = combined[columns]
            
            # 按日期與訂單號排序
            if 'order_date' in combined.columns and 'order_sn' in combined.columns:
                combined = combined[combined['order_date'].str.strip() != ''].copy()
                combined.sort_values(by=['order_date', 'order_sn'], inplace=True)
                combined.reset_index(drop=True, inplace=True)
                
            combined.to_csv(self.output_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"已儲存：{self.output_path} ({len(combined)} 筆)")
            
        except Exception as e:
            self.logger.error(f"儲存資料失敗：{e}")
            raise
    
    def cleanup_temp_files(self):
        """清理暫存檔案"""
        c1105_files = glob(str(self.source_dir / "C1105_*.csv"))
        
        if c1105_files:
            self.logger.info("正在清理暫存檔案...")
            for file_path in c1105_files:
                try:
                    os.unlink(file_path)
                    self.logger.info(f"已刪除：{Path(file_path).name}")
                except OSError as e:
                    self.logger.warning(f"刪除失敗：{Path(file_path).name} - {e}")
    
    def run(self):
        """執行主程式"""
        try:
            self.logger.info("MOMO 帳務對帳訂單清理腳本啟動")
            
            # 載入 mapping
            mapping, columns = self.get_mapping()
            
            # 讀取 CSV 檔案
            df = self.read_csv_files(mapping)
            if df.empty:
                self.logger.warning("沒有找到任何可處理的檔案")
                return
            
            # 處理資料
            processed_df = self.process_data(df, mapping, columns)
            
            # 儲存資料
            self.save_data(processed_df, columns)
            
            # 清理暫存檔案
            self.cleanup_temp_files()
            
            self.logger.info("=== 帳務對帳訂單清理完成 ===")
            
        except Exception as e:
            self.logger.error(f"程式執行失敗：{e}")
            raise
        finally:
            self.logger.info("=== 程式結束 ===")

def main():
    """主函式"""
    cleaner = MomoAccountingCleaner()
    cleaner.run()

if __name__ == "__main__":
    main()