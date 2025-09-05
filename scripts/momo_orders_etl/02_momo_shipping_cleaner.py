# scripts/momo_orders_etl/momo_shipping_cleaner.py
# -*- coding: utf-8 -*-
"""
MOMO 出貨管理訂單清理腳本 (A1102/A1106 系列)

功能：
- 批次讀取 temp/momo/A1102_2_*.csv, A1102_3_*.csv 和 A1106_*.csv 檔案
- 按 a1102_momo_fields_mapping.json 定義調整欄位與順序
- 自動轉換 A1106 格式為 A1102 標準格式
- 合併 A1102_2, A1102_3 和 A1106 資料 (A1102_3 > A1106 > A1102_2 優先級)
- 輸出到 data_processed/merged/momo_shipping_orders_cleaned.csv

使用：python scripts/momo_orders_etl/momo_shipping_cleaner.py

輸入：
- temp/momo/A1102_2_*.csv, A1102_3_*.csv, A1106_*.csv 檔案
- config/a1102_momo_fields_mapping.json

輸出：
- data_processed/merged/momo_shipping_orders_cleaned.csv

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
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
        # 路徑設定 - 腳本在 scripts/momo_orders_etl/ 目錄下
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parents[1]  # 向上兩層到達專案根目錄
        
        # 檔案路徑
        self.mapping_path = self.project_root / "config" / "a1102_momo_fields_mapping.json"
        self.source_dir = self.project_root / "data_raw" / "momo"
        self.output_dir = self.project_root / "temp" / "momo"
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
        
        # 設定檔案 handler
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 設定控制台 handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 設定根 logger
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler],
            force=True
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== MOMO 出貨管理訂單清理開始 (支援 A1106) ===")
        
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
    
    # 移除 A1106 相關函數，因為新腳本只處理 CSV 檔案
    
    def read_csv_files(self, mapping):
        """讀取 A1102 檔案，支援新的命名格式"""
        # 搜尋 A1102 開頭的 CSV 檔案（新命名格式）
        patterns = ["A1102_2_超商取貨_*.csv", "A1102_3_第三方物流_*.csv"]
        all_files = []
        for pattern in patterns:
            all_files.extend(glob(str(self.source_dir / pattern)))

        if not all_files:
            self.logger.warning(f"在 {self.source_dir} 目錄下沒有找到任何可處理的檔案")
            return pd.DataFrame()

        # 統計各類型檔案數量
        def count_files(prefix):
            return len([f for f in all_files if Path(f).name.startswith(prefix)])
        self.logger.info(f"找到 {count_files('A1102_2_超商取貨_')} 個超商取貨檔案")
        self.logger.info(f"找到 {count_files('A1102_3_第三方物流_')} 個第三方物流檔案")

        # 建立中文到英文的欄位對應
        zh_to_en = {v["zh_name"]: k for k, v in mapping.items() if "zh_name" in v}

        dfs = []
        for file_path in all_files:
            try:
                file_name = Path(file_path).name
                self.logger.info(f"處理檔案：{file_name}")

                # 讀取 CSV 檔案
                df = None
                encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, dtype=str, encoding=encoding).fillna("")
                        self.logger.info(f"成功使用編碼：{encoding}")
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        self.logger.warning(f"編碼 {encoding} 讀取失敗：{e}")
                        continue
                if df is None:
                    self.logger.error(f"無法讀取檔案：{file_name}，所有編碼都失敗")
                    continue

                # 根據檔案名稱判斷資料來源
                df = df.rename(columns=zh_to_en)
                
                # 額外的欄位重新命名（處理英文欄位名稱）
                field_rename_map = {
                    'product_cost': 'platform_product_cost'
                }
                df = df.rename(columns=field_rename_map)
                if file_name.startswith("A1102_2_超商取貨_"):
                    data_source = 'A1102_2'
                elif file_name.startswith("A1102_3_第三方物流_"):
                    data_source = 'A1102_3'
                else:
                    data_source = 'A1102'

                if 'order_sn' in df.columns:
                    df = df[df['order_sn'].str.strip() != ""]

                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                        if col in ['product_sku_main', 'quantity', 'product_manufacturer_code']:
                            df[col] = df[col].str.replace(r'\.0$', '', regex=True)

                df['data_source'] = data_source
                dfs.append(df)
                self.logger.info(f"讀取成功：{file_name} ({len(df)} 筆)")
            except Exception as e:
                self.logger.error(f"讀取失敗：{file_path} - {e}")

        if not dfs:
            return pd.DataFrame()
        combined_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"總共讀取 {len(combined_df)} 筆原始資料")
        return combined_df
    
    def merge_data_by_priority(self, df):
        """按優先級合併資料：A1102_3 > A1102_2"""
        if 'data_source' not in df.columns or 'order_sn' not in df.columns:
            return df
            
        # 分離不同資料來源
        a1102_2_df = df[df['data_source'] == 'A1102_2'].copy()
        a1102_3_df = df[df['data_source'] == 'A1102_3'].copy()
        
        self.logger.info("開始按優先級合併資料...")
        self.logger.info(f"A1102_2: {len(a1102_2_df)} 筆")
        self.logger.info(f"A1102_3: {len(a1102_3_df)} 筆") 
        
        # 按優先級合併：A1102_3 > A1102_2
        merged_df = pd.DataFrame()
        used_orders = set()
        
        # 第一優先：A1102_3
        if not a1102_3_df.empty:
            merged_df = pd.concat([merged_df, a1102_3_df], ignore_index=True)
            used_orders.update(a1102_3_df['order_sn'].values)
            self.logger.info(f"加入 A1102_3 資料：{len(a1102_3_df)} 筆")
        
        # 第二優先：A1102_2 (排除已有的訂單)
        if not a1102_2_df.empty:
            a1102_2_unique = a1102_2_df[~a1102_2_df['order_sn'].isin(used_orders)]
            if not a1102_2_unique.empty:
                merged_df = pd.concat([merged_df, a1102_2_unique], ignore_index=True)
                self.logger.info(f"加入 A1102_2 獨有資料：{len(a1102_2_unique)} 筆")
        
        # 如果都沒有資料，返回原始資料
        if merged_df.empty:
            merged_df = df
        
        self.logger.info(f"合併後共 {len(merged_df)} 筆資料")
        return merged_df
    
    def standardize_date_format(self, date_str):
        """標準化日期格式：YYYY/MM/DD -> YYYY-MM-DD"""
        if not isinstance(date_str, str) or not date_str.strip():
            return ""
        
        try:
            # 處理 YYYY/MM/DD 格式
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) == 3:
                    year, month, day = parts
                    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            
            # 如果已經是 YYYY-MM-DD 格式，直接返回
            if '-' in date_str and len(date_str) == 10:
                return date_str
                
            return date_str
        except:
            return ""
    
    def process_data(self, df, mapping, columns):
        """根據 mapping 處理資料"""
        self.logger.info("開始資料處理...")
        
        # 按優先級合併資料
        df = self.merge_data_by_priority(df)
        
        # 確保必要欄位存在
        if 'platform' not in df.columns:
            df['platform'] = 'momo'
        if 'processing_date' not in df.columns:
            df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 解析訂單日期 (如果還沒有的話)
        if 'order_date' not in df.columns or df['order_date'].isna().all():
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
        
        # 解析訂單編號組成 (如果還沒有的話)
        missing_fields_for_parse = ['order_line_number', 'order_sub_sequence', 'order_detail_sequence']
        # Only parse if any of the *relevant* fields are missing.
        # order_sn_main is usually parsed separately or derived from order_sn.
        
        if 'order_sn' in df.columns and any(field not in df.columns for field in missing_fields_for_parse):
            def parse_order_sn_components_for_all(order_sn):
                if isinstance(order_sn, str) and '-' in order_sn:
                    parts = order_sn.split('-')
                    order_sn_main = parts[0]
                    order_line = parts[1] if len(parts) > 1 else '001'
                    sub_seq = parts[2] if len(parts) > 2 else '001'
                    detail_seq = parts[3] if len(parts) > 3 else '001'
                    return pd.Series([order_sn_main, order_line, sub_seq, detail_seq])
                return pd.Series([order_sn, '001', '001', '001']) # Default if not split by '-'

            # Ensure all four components are assigned correctly
            # This handles cases where convert_a1106_to_a1102 might not have run or if A1102 files don't have these.
            df[['order_sn_main', 'order_line_number', 'order_sub_sequence', 'order_detail_sequence']] = \
                df['order_sn'].apply(parse_order_sn_components_for_all)

        # 判斷是否為異常單 (如果還沒有的話)
        if 'is_abnormal_order' not in df.columns:
            def is_abnormal(row):
                try:
                    # 檢查 order_sub_sequence 和 order_detail_sequence 是否都是 "001"
                    # 排除 order_line_number (第一個001) 的判斷
                    sub_seq = str(row.get('order_sub_sequence', '001'))
                    detail_seq = str(row.get('order_detail_sequence', '001'))
                    
                    # 如果其中一個不是 "001"，就是異常訂單
                    return not (sub_seq == "001" and detail_seq == "001")
                except:
                    return True # 異常情況返回 True
            
            # This should now correctly use the newly parsed or existing 'order_sub_sequence' and 'order_detail_sequence'
            if 'order_sub_sequence' in df.columns and 'order_detail_sequence' in df.columns:
                df['is_abnormal_order'] = df.apply(is_abnormal, axis=1)
            else:
                self.logger.warning("缺少 'order_sub_sequence' 或 'order_detail_sequence' 欄位，無法判斷 'is_abnormal_order'")
                df['is_abnormal_order'] = False # Default to False if fields are missing

        # 生成合併鍵 (如果還沒有的話)
        if 'key_for_merge' not in df.columns:
            if 'order_sn' in df.columns:
                df['key_for_merge'] = 'momo_' + df['order_sn'].astype(str)
        
        # 處理日期欄位格式標準化
        date_fields = ['invoice_date', 'ship_by_date']
        for field in date_fields:
            if field in df.columns:
                self.logger.info(f"標準化日期欄位：{field}")
                df[field] = df[field].apply(self.standardize_date_format)
        
        # 處理日期時間欄位
        datetime_fields = ['order_transfer_date']
        for field in datetime_fields:
            if field in df.columns:
                def standardize_datetime(dt_str):
                    if not isinstance(dt_str, str) or not dt_str.strip():
                        return ""
                    try:
                        # 處理 YYYY/MM/DD HH:MM 格式
                        if '/' in dt_str:
                            if ' ' in dt_str:  # 有時間部分
                                date_part, time_part = dt_str.split(' ', 1)
                                parts = date_part.split('/')
                                if len(parts) == 3:
                                    year, month, day = parts
                                    formatted_date = f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
                                    return f"{formatted_date} {time_part}"
                            else:  # 只有日期部分
                                return self.standardize_date_format(dt_str)
                        return dt_str
                    except:
                        return ""
                
                self.logger.info(f"標準化日期時間欄位：{field}")
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
                    # 日期欄位已經在上面處理過格式標準化
                    pass
                
                elif data_type in ['DATETIME', 'TIMESTAMP']:
                    # BigQuery DATETIME/TIMESTAMP 類型
                    # 日期時間欄位已經在上面處理過格式標準化
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
            
            # 最終重複檢查和處理
            has_duplicates = self.check_duplicate_order_sn(combined)
            if has_duplicates:
                combined = self.handle_duplicates(combined)
                # 再次檢查確認去重成功
                self.check_duplicate_order_sn(combined)
            
            # 強制重新排序欄位
            combined = combined[columns]
            
            # 處理帳務數字欄位，確保小數點下兩位
            cost_fields = ['product_cost_untaxed', 'platform_product_cost', 'product_original_price']
            for field in cost_fields:
                if field in combined.columns:
                    # 轉換為數值，保留小數點下兩位
                    combined[field] = pd.to_numeric(combined[field], errors='coerce').round(2)
            
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
    
    def check_duplicate_order_sn(self, df):
        """檢查 order_sn 重複情況"""
        self.logger.info("檢查 order_sn 重複情況...")
        
        if 'order_sn' not in df.columns:
            self.logger.warning("缺少 order_sn 欄位，跳過重複檢查")
            return False
        
        # 統計重複情況
        total_records = len(df)
        unique_order_sns = df['order_sn'].nunique()
        duplicate_count = total_records - unique_order_sns
        
        if duplicate_count == 0:
            self.logger.info(f"未發現重複的 order_sn (總計 {total_records} 筆，全部唯一)")
            return False
        
        # 找出重複的 order_sn
        duplicate_order_sns = df[df.duplicated(subset=['order_sn'], keep=False)]['order_sn'].unique()
        
        self.logger.warning(f"發現 {duplicate_count} 筆重複資料")
        self.logger.warning(f"重複統計：")
        self.logger.warning(f"   - 總筆數：{total_records}")
        self.logger.warning(f"   - 唯一 order_sn：{unique_order_sns}")
        self.logger.warning(f"   - 重複筆數：{duplicate_count}")
        self.logger.warning(f"   - 重複的 order_sn 數量：{len(duplicate_order_sns)}")
        
        # 顯示前幾個重複的 order_sn 作為範例
        sample_duplicates = duplicate_order_sns[:5]
        self.logger.warning(f"   - 重複 order_sn 範例：{list(sample_duplicates)}")
        if len(duplicate_order_sns) > 5:
            self.logger.warning(f"   - ... 還有 {len(duplicate_order_sns) - 5} 個重複的 order_sn")
        
        # 詳細分析每個重複的 order_sn
        self.logger.info("重複資料詳細分析：")
        for order_sn in sample_duplicates:
            duplicates = df[df['order_sn'] == order_sn]
            data_sources = duplicates['data_source'].value_counts().to_dict() if 'data_source' in duplicates.columns else {}
            self.logger.info(f"   - {order_sn}: {len(duplicates)} 筆重複 {data_sources}")
        
        return True
    
    def handle_duplicates(self, df):
        """處理重複資料"""
        if 'order_sn' not in df.columns:
            return df
        
        self.logger.info("處理重複資料...")
        
        # 按優先級去重：data_source 優先級 A1102_3 > A1106 > A1102_2
        priority_map = {'A1102_3': 3, 'A1106': 2, 'A1102_2': 1, 'A1102': 1}
        
        if 'data_source' in df.columns:
            # 添加優先級欄位
            df['priority'] = df['data_source'].map(priority_map).fillna(0)
            
            # 按 order_sn 分組，保留優先級最高的記錄
            df_dedup = df.loc[df.groupby('order_sn')['priority'].idxmax()].copy()
            
            # 移除臨時的優先級欄位
            df_dedup = df_dedup.drop('priority', axis=1)
            
            removed_count = len(df) - len(df_dedup)
            if removed_count > 0:
                self.logger.info(f"已移除 {removed_count} 筆重複資料 (保留高優先級來源)")
                self.logger.info(f"去重後剩餘 {len(df_dedup)} 筆唯一資料")
            
            return df_dedup
        else:
            # 如果沒有 data_source，簡單去重保留最後一筆
            df_dedup = df.drop_duplicates(subset=['order_sn'], keep='last')
            removed_count = len(df) - len(df_dedup)
            if removed_count > 0:
                self.logger.info(f"已移除 {removed_count} 筆重複資料 (保留最後一筆)")
            
            return df_dedup
    
    def cleanup_temp_files(self, force_cleanup=False):
        """
        清理暫存檔案
        
        Args:
            force_cleanup (bool): 是否強制清理，預設為 False
        """
        # 定義要清理的檔案模式
        file_patterns = [
            "A1102_2_*.csv",
            "A1102_3_*.csv", 
            "A1106_*.csv"
        ]
        
        all_files = []
        for pattern in file_patterns:
            files = glob(str(self.source_dir / pattern))
            all_files.extend(files)
        
        if not all_files:
            self.logger.info("沒有找到需要清理的暫存檔案")
            return
        
        # 顯示找到的檔案
        self.logger.info(f"找到 {len(all_files)} 個暫存檔案：")
        for file_path in all_files:
            file_size = os.path.getsize(file_path) / 1024  # KB
            self.logger.info(f"  - {Path(file_path).name} ({file_size:.1f} KB)")
        
        # 執行清理（預設執行，除非明確設定 force_cleanup=False）
        if not force_cleanup:
            self.logger.info("正在清理暫存檔案...")
        else:
            self.logger.info("強制清理暫存檔案...")
        
        deleted_count = 0
        failed_count = 0
        
        for file_path in all_files:
            try:
                file_size = os.path.getsize(file_path) / 1024  # KB
                os.unlink(file_path)
                self.logger.info(f"已刪除：{Path(file_path).name} ({file_size:.1f} KB)")
                deleted_count += 1
            except OSError as e:
                self.logger.warning(f"刪除失敗：{Path(file_path).name} - {e}")
                failed_count += 1
        
        # 清理結果報告
        self.logger.info(f"清理完成：")
        self.logger.info(f"  - 成功刪除：{deleted_count} 個檔案")
        if failed_count > 0:
            self.logger.warning(f"  - 刪除失敗：{failed_count} 個檔案")
        
        # 檢查目錄是否為空
        remaining_files = []
        for pattern in file_patterns:
            files = glob(str(self.source_dir / pattern))
            remaining_files.extend(files)
        
        if not remaining_files:
            self.logger.info("暫存目錄已完全清理")
        else:
            self.logger.warning(f"仍有 {len(remaining_files)} 個檔案未清理")
    
    def check_temp_files_status(self):
        """檢查暫存檔案狀態"""
        file_patterns = [
            "A1102_2_*.csv",
            "A1102_3_*.csv", 
            "A1106_*.csv"
        ]
        
        all_files = []
        for pattern in file_patterns:
            files = glob(str(self.source_dir / pattern))
            all_files.extend(files)
        
        if not all_files:
            self.logger.info("暫存目錄中沒有找到任何 CSV 檔案")
            return
        
        total_size = 0
        self.logger.info(f"暫存目錄狀態：找到 {len(all_files)} 個檔案")
        
        for file_path in all_files:
            file_size = os.path.getsize(file_path)
            total_size += file_size
            size_kb = file_size / 1024
            self.logger.info(f"  - {Path(file_path).name} ({size_kb:.1f} KB)")
        
        total_size_mb = total_size / 1024 / 1024
        self.logger.info(f"總計：{len(all_files)} 個檔案，{total_size_mb:.1f} MB")
    
    def test_abnormal_order_logic(self):
        """測試異常訂單判斷邏輯"""
        test_cases = [
            # 正常訂單 (兩個都是 001)
            ("001", "001", False),  # 正常：兩個都是 001
            ("001", "002", True),   # 異常：detail_sequence 不是 001
            ("002", "001", True),   # 異常：sub_sequence 不是 001
            ("002", "002", True),   # 異常：兩個都不是 001
            
            # 邊界情況
            ("", "001", True),      # 異常：sub_sequence 為空
            ("001", "", True),      # 異常：detail_sequence 為空
            ("", "", True),         # 異常：兩個都為空
            ("abc", "001", True),   # 異常：sub_sequence 不是數字
            ("001", "abc", True),   # 異常：detail_sequence 不是數字
        ]
        
        def is_abnormal_test(sub_seq, detail_seq):
            try:
                # 檢查 order_sub_sequence 和 order_detail_sequence 是否都是 "001"
                sub_seq_str = str(sub_seq)
                detail_seq_str = str(detail_seq)
                
                # 如果其中一個不是 "001"，就是異常訂單
                return not (sub_seq_str == "001" and detail_seq_str == "001")
            except:
                return True  # 異常情況返回 True
        
        self.logger.info("=== 測試異常訂單判斷邏輯 ===")
        for sub_seq, detail_seq, expected in test_cases:
            result = is_abnormal_test(sub_seq, detail_seq)
            status = "PASS" if result == expected else "FAIL"
            self.logger.info(f"{status} sub_seq='{sub_seq}', detail_seq='{detail_seq}' -> 預期:{expected}, 實際:{result}")
    
    def run(self, cleanup_temp=True, force_cleanup=False):
        """
        執行主程式
        
        Args:
            cleanup_temp (bool): 是否清理暫存檔案，預設為 True
            force_cleanup (bool): 是否強制清理，預設為 False
        """
        try:
            self.logger.info("MOMO 出貨管理訂單清理腳本啟動 (支援 A1106)")
            
            # 載入 mapping (同時建立 A1106 欄位對應)
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
            if cleanup_temp:
                self.logger.info("執行暫存檔案清理...")
                self.cleanup_temp_files(force_cleanup=force_cleanup)
            else:
                self.logger.info("跳過暫存檔案清理")
            
            self.logger.info("=== 出貨管理訂單清理完成 ===")
            
        except Exception as e:
            self.logger.error(f"程式執行失敗：{e}")
            raise
        finally:
            self.logger.info("=== 程式結束 ===")

def main():
    """主函式"""
    import argparse
    
    parser = argparse.ArgumentParser(description="MOMO 出貨管理訂單清理腳本")
    parser.add_argument("--no-cleanup", action="store_true", help="不清理暫存檔案")
    parser.add_argument("--force-cleanup", action="store_true", help="強制清理暫存檔案")
    parser.add_argument("--cleanup-only", action="store_true", help="僅執行清理功能")
    parser.add_argument("--check-status", action="store_true", help="檢查暫存檔案狀態")
    parser.add_argument("--test-logic", action="store_true", help="測試異常訂單判斷邏輯")
    
    args = parser.parse_args()
    
    cleaner = MomoShippingCleaner()
    
    if args.test_logic:
        # 測試異常訂單判斷邏輯
        cleaner.logger.info("=== 測試異常訂單判斷邏輯 ===")
        cleaner.test_abnormal_order_logic()
    elif args.check_status:
        # 檢查暫存檔案狀態
        cleaner.logger.info("=== 檢查暫存檔案狀態 ===")
        cleaner.check_temp_files_status()
    elif args.cleanup_only:
        # 僅執行清理功能
        cleaner.logger.info("=== 僅執行暫存檔案清理 ===")
        cleaner.cleanup_temp_files(force_cleanup=args.force_cleanup)
    else:
        # 執行完整流程
        cleanup_temp = not args.no_cleanup
        cleaner.run(cleanup_temp=cleanup_temp, force_cleanup=args.force_cleanup)

if __name__ == "__main__":
    main()