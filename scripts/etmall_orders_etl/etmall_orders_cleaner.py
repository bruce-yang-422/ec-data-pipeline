# scripts/etmall_orders_etl/etmall_orders_cleaner.py
# -*- coding: utf-8 -*-
"""
東森購物訂單資料庫清洗腳本
"""

import pandas as pd
import json
import sys
import logging
from datetime import datetime
from pathlib import Path
import re

def is_valid_date(val):
    if isinstance(val, str) and re.match(r'^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}(:\d{2})?)?$', val.strip()):
        return True
    return False

class EtmallOrdersCleaner:
    def __init__(self):
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parents[1]
        self.mapping_path = self.project_root / "config" / "etmall_fields_mapping.json"
        self.source_dir = self.project_root / "data_raw" / "etmall"
        self.output_dir = self.project_root / "data_processed" / "merged"
        self.output_path = self.output_dir / "etmall_orders_cleaned.csv"
        self.logs_dir = self.project_root / "logs"
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.setup_logging()

    def setup_logging(self):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"etmall_orders_cleaner_{timestamp}.log"
        log_path = self.logs_dir / log_filename
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        logging.basicConfig(level=logging.INFO, handlers=[file_handler, console_handler])
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== 東森購物訂單資料庫清洗開始 ===")

    def get_mapping(self):
        with open(self.mapping_path, "r", encoding="utf-8") as f:
            mapping = json.load(f)
        mapping = {k.strip(): v for k, v in mapping.items()}
        columns = []
        for k, v in sorted(mapping.items(), key=lambda item: int(item[1]["order"])):
            if k not in columns:
                columns.append(k)
        self.logger.info(f"載入 mapping 配置：{len(mapping)} 個欄位, 欄位順序: {columns}")
        return mapping, columns

    def read_csv_files(self) -> pd.DataFrame:
        csv_files = list(self.source_dir.glob("*.csv"))
        if not csv_files:
            self.logger.warning(f"在 {self.source_dir} 目錄下沒有找到 CSV 檔案")
            return pd.DataFrame()
        self.logger.info(f"找到 {len(csv_files)} 個 CSV 檔案")
        dfs = []
        for file_path in csv_files:
            try:
                file_name = file_path.name
                self.logger.info(f"處理檔案：{file_name}")
                df = None
                encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, dtype=str, encoding=encoding)
                        df.columns = [col.strip() for col in df.columns]
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
                df = df.fillna('')
                df = df.replace(['', 'nan', 'NaN'], '')
                for col in df.columns:
                    if df[col].dtype == 'object':
                        df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                df['data_source'] = file_name
                dfs.append(df)
                self.logger.info(f"讀取成功：{file_name} ({len(df)} 筆)")
            except Exception as e:
                self.logger.error(f"讀取失敗：{file_path} - {e}")
        if not dfs:
            return pd.DataFrame()
        combined_df = pd.concat(dfs, ignore_index=True)
        self.logger.info(f"總共讀取 {len(combined_df)} 筆原始資料")
        return combined_df

    def map_columns(self, df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
        zh2en = {v["zh_name"].strip(): en for en, v in mapping.items() if "zh_name" in v}
        rename_dict = {}
        for col in df.columns:
            col_strip = col.strip()
            if col_strip in zh2en:
                rename_dict[col] = zh2en[col_strip]
        df = df.rename(columns=rename_dict)
        df = df.loc[:, ~df.columns.duplicated()]
        self.logger.info(f"map_columns後欄位: {df.columns.tolist()}")
        return df

    def generate_order_line_uid(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'order_sn' in df.columns and 'line_number' in df.columns:
            df['order_line_uid'] = df['order_sn'].astype(str) + '_' + df['line_number'].astype(str)
        elif 'order_sn' in df.columns:
            df['order_line_uid'] = df['order_sn'].astype(str)
            df['line_number'] = 1
        else:
            self.logger.warning("無法生成 order_line_uid，缺少必要欄位")
            df['order_line_uid'] = ''
            df['line_number'] = 1
        return df

    def standardize_date_format(self, date_str):
        if not isinstance(date_str, str) or not date_str.strip():
            return ""
        try:
            date_str = str(date_str).strip()
            if len(date_str) >= 10 and '-' in date_str:
                return date_str
            if '/' in date_str:
                parts = date_str.split('/')
                if len(parts) >= 3:
                    year, month, day = parts[:3]
                    return f"{int(year):04d}-{int(month):02d}-{int(day):02d}"
            if len(date_str) == 8 and date_str.isdigit():
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                return f"{year}-{month}-{day}"
            return date_str
        except:
            return ""

    def process_data(self, df: pd.DataFrame, mapping: dict, columns: list) -> pd.DataFrame:
        self.logger.info("開始資料處理...")
        df['platform'] = 'etmall'
        df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # === 只補中文原始欄位（mapping.zh_name），不補英文 key ===
        zh_names = [v["zh_name"].strip() for k, v in mapping.items() if "zh_name" in v]
        for zh in zh_names:
            if zh not in df.columns:
                df[zh] = ''
        self.logger.info(f"process_data補齊欄位後: {df.columns.tolist()}")

        # mapping 中文轉英文
        df = self.map_columns(df, mapping)

        # === 補英文欄位（如程式自動生成欄位：order_line_uid、platform...）===
        for col in columns:
            if col not in df.columns:
                if col == 'shipping_confirmation_date':
                    df[col] = ''
                elif col in ['unit_price', 'cost']:
                    df[col] = 0
                elif col in ['quantity']:
                    df[col] = 1
                elif col in ['line_number']:
                    df[col] = 1
                else:
                    df[col] = ''

        df = self.generate_order_line_uid(df)
        if 'order_date' in df.columns:
            self.logger.info("標準化訂單日期格式")
            df['order_date'] = df['order_date'].apply(self.standardize_date_format)
        numeric_fields = ['unit_price', 'cost', 'quantity']
        for field in numeric_fields:
            if field in df.columns:
                try:
                    df[field] = pd.to_numeric(df[field], errors='coerce')
                    if field == 'quantity':
                        df[field] = df[field].fillna(1).astype(int)
                    else:
                        df[field] = df[field].fillna(0)
                except Exception as e:
                    self.logger.warning(f"處理數值欄位 {field} 時發生錯誤：{e}")

        if 'order_sn' in df.columns:
            before_filter = len(df)
            df = df[df['order_sn'].astype(str).str.strip() != ""]
            self.logger.info(f"過濾空訂單編號：{before_filter} -> {len(df)} 筆")

        self.logger.info(f"資料處理完成，共 {len(df)} 筆資料")
        return df

    def check_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        if 'order_line_uid' not in df.columns:
            self.logger.warning("無法檢查重複，缺少 order_line_uid 欄位")
            return df
        before_dedup = len(df)
        duplicates = df[df['order_line_uid'].duplicated(keep=False)]
        if len(duplicates) > 0:
            self.logger.warning(f"發現 {len(duplicates)} 筆重複資料")
            df = df.sort_values('processing_date', ascending=False).drop_duplicates(
                subset=['order_line_uid'], keep='first'
            )
            self.logger.info(f"去重後：{before_dedup} -> {len(df)} 筆")
        else:
            self.logger.info("無重複資料")
        return df

    def merge_with_existing(self, df: pd.DataFrame) -> pd.DataFrame:
        if not self.output_path.exists():
            self.logger.info("輸出檔案不存在，直接儲存新資料")
            return df
        try:
            existing_df = pd.read_csv(self.output_path, dtype=str)
            self.logger.info(f"讀取現有資料：{len(existing_df)} 筆")
            combined_df = pd.concat([existing_df, df], ignore_index=True)
            self.logger.info(f"合併後資料：{len(combined_df)} 筆")
            combined_df = self.check_duplicates(combined_df)
            if 'shipping_confirmation_date' in combined_df.columns:
                combined_df['shipping_confirmation_date'] = combined_df['shipping_confirmation_date'].astype(str)
                combined_df['shipping_confirmation_date'] = combined_df['shipping_confirmation_date'].replace(['nan', 'NaN', 'NaT', 'None'], '').fillna('')
            return combined_df
        except Exception as e:
            self.logger.error(f"合併現有資料時發生錯誤：{e}")
            return df

    def save_data(self, df: pd.DataFrame, columns: list, mapping: dict):
        try:
            self.output_dir.mkdir(parents=True, exist_ok=True)
            # 欄位唯一且順序正確
            df_columns_seen = set()
            available_columns = []
            for col in columns:
                if col in df.columns and col not in df_columns_seen:
                    available_columns.append(col)
                    df_columns_seen.add(col)
            df_output = df[available_columns].copy()
            self.logger.info(f"save_data 欄位: {df_output.columns.tolist()}")
            if 'shipping_confirmation_date' in df_output.columns:
                df_output['shipping_confirmation_date'] = df_output['shipping_confirmation_date'].astype(str)
                df_output['shipping_confirmation_date'] = df_output['shipping_confirmation_date'].replace(['nan', 'NaN', 'NaT', 'None'], '').fillna('')
            if 'order_date' in df_output.columns:
                # 排序時用 to_datetime，但欄位內容本身不會填預設
                df_output = df_output.sort_values('order_date', key=lambda x: pd.to_datetime(x, errors='coerce'), ascending=True, na_position='first')
                # 格式化顯示
                def fmt_date(x):
                    try:
                        dt = pd.to_datetime(x, errors='coerce')
                        return dt.strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(dt) else ''
                    except:
                        return ''
                df_output['order_date'] = df_output['order_date'].apply(fmt_date)
            df_output.to_csv(self.output_path, index=False, encoding='utf-8-sig')
            self.logger.info(f"✅ 資料儲存成功：{self.output_path}")
            self.logger.info(f"📊 資料統計：{len(df_output)} 筆，{len(df_output.columns)} 欄")
        except Exception as e:
            self.logger.error(f"儲存資料時發生錯誤：{e}")
            raise

    def run(self):
        try:
            mapping, columns = self.get_mapping()
            df = self.read_csv_files()
            if df.empty:
                self.logger.warning("沒有找到可處理的資料")
                return
            df = self.process_data(df, mapping, columns)
            df = self.merge_with_existing(df)
            self.save_data(df, columns, mapping)
            self.logger.info("=== 東森購物訂單資料庫清洗完成 ===")
        except Exception as e:
            self.logger.error(f"執行過程中發生錯誤：{e}")
            raise

def main():
    cleaner = EtmallOrdersCleaner()
    cleaner.run()

if __name__ == "__main__":
    main()
