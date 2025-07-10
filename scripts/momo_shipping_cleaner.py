# scripts/momo_shipping_cleaner.py
# -*- coding: utf-8 -*-
"""
MOMO 出貨管理訂單清理腳本 (A1102 系列)

功能：
- 批次讀取 temp/momo/A1102_2_*.csv 和 temp/momo/A1102_3_*.csv 檔案
- 按 a1102_momo_fields_mapping.json 定義調整欄位與順序
- 合併 A1102_2 和 A1102_3 資料 (A1102_3 優先)
- 輸出到 data_processed/merged/momo_shipping_orders_cleaned.csv

使用：python scripts/momo_shipping_cleaner.py
"""

import pandas as pd
import json
import os
import sys
import logging
from datetime import datetime
from glob import glob
from pathlib import Path

class MomoShippingCleaner:
    def __init__(self):
        # 路徑設定
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parent
        
        # 檔案路徑
        self.mapping_path = self.project_root / "config" / "a1102_momo_fields_mapping.json"
        self.source_dir = self.project_root / "temp" / "momo"
        self.output_dir = self.project_root / "data_processed" / "merged"
        self.output_path = self.output_dir / "momo_shipping_orders_cleaned.csv"
        self.logs_dir = self.project_root / "logs"
        
        # 確保目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
    def setup_logging(self):
        """設定日誌系統"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"momo_shipping_cleaner_{timestamp}.log"
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
        self.logger.info("=== MOMO 出貨管理訂單清理開始 ===")
        
    def get_mapping(self):
        """讀取 A1102 mapping 設定並根據 'order' 欄位排序"""
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
        """讀取 A1102_2 和 A1102_3 CSV 檔案"""
        # 尋找 A1102_2 和 A1102_3 開頭的檔案
        a1102_2_files = glob(str(self.source_dir / "A1102_2_*.csv"))
        a1102_3_files = glob(str(self.source_dir / "A1102_3_*.csv"))
        
        all_files = a1102_2_files + a1102_3_files
        
        if not all_files:
            self.logger.warning(f"在 {self.source_dir} 目錄下沒有找到 A1102 CSV 檔案")
            return pd.DataFrame()
        
        self.logger.info(f"找到 {len(a1102_2_files)} 個 A1102_2 檔案")
        self.logger.info(f"找到 {len(a1102_3_files)} 個 A1102_3 檔案")
        
        # 建立中文到英文的欄位對應
        zh_to_en = {v["zh_name"]: k for k, v in mapping.items()}
        
        dfs = []
        for file_path in all_files:
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
                if file_name.startswith("A1102_2_"):
                    df['data_source'] = 'A1102_2'
                elif file_name.startswith("A1102_3_"):
                    df['data_source'] = 'A1102_3'
                
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
    
    def merge_a1102_data(self, df):
        """合併 A1102_2 和 A1102_3 資料，A1102_3 優先"""
        if 'data_source' not in df.columns or 'order_sn' not in df.columns:
            return df
            
        # 分離兩種資料來源
        a1102_2_df = df[df['data_source'] == 'A1102_2'].copy()
        a1102_3_df = df[df['data_source'] == 'A1102_3'].copy()
        
        if a1102_3_df.empty:
            self.logger.info("只有 A1102_2 資料，直接使用")
            return a1102_2_df
        
        if a1102_2_df.empty:
            self.logger.info("只有 A1102_3 資料，直接使用")
            return a1102_3_df
        
        # A1102_3 為主，A1102_2 補充
        self.logger.info("合併 A1102_2 和 A1102_3 資料...")
        
        # 以 A1102_3 為基礎
        merged_df = a1102_3_df.copy()
        
        # 找出 A1102_3 中沒有的訂單編號
        a1102_3_orders = set(a1102_3_df['order_sn'].values)
        a1102_2_unique = a1102_2_df[~a1102_2_df['order_sn'].isin(a1102_3_orders)]
        
        if not a1102_2_unique.empty:
            merged_df = pd.concat([merged_df, a1102_2_unique], ignore_index=True)
            self.logger.info(f"從 A1102_2 補充了 {len(a1102_2_unique)} 筆獨有資料")
        
        self.logger.info(f"合併後共 {len(merged_df)} 筆資料")
        return merged_df
    
    def process_data(self, df, mapping, columns):
        """根據 mapping 處理資料"""
        self.logger.info("開始資料處理...")
        
        # 合併 A1102 資料
        df = self.merge_a1102_data(df)
        
        # 添加固定欄位
        df['platform'] = 'momo'
        df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 解析訂單日期
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
        a1102_files = glob(str(self.source_dir / "A1102_2_*.csv")) + \
                     glob(str(self.source_dir / "A1102_3_*.csv"))
        
        if a1102_files:
            self.logger.info("正在清理暫存檔案...")
            for file_path in a1102_files:
                try:
                    os.unlink(file_path)
                    self.logger.info(f"已刪除：{Path(file_path).name}")
                except OSError as e:
                    self.logger.warning(f"刪除失敗：{Path(file_path).name} - {e}")
    
    def run(self):
        """執行主程式"""
        try:
            self.logger.info("MOMO 出貨管理訂單清理腳本啟動")
            
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
            
            self.logger.info("=== 出貨管理訂單清理完成 ===")
            
        except Exception as e:
            self.logger.error(f"程式執行失敗：{e}")
            raise
        finally:
            self.logger.info("=== 程式結束 ===")

def main():
    """主函式"""
    cleaner = MomoShippingCleaner()
    cleaner.run()

if __name__ == "__main__":
    main()