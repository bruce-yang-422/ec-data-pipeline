#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
06_momo_orders_bq_formatter.py

功能：
- 讀取 05_momo_orders_shop_enricher.py 輸出的豐富化檔案
- 轉換為 BigQuery 相容格式
- 輸出到 data_processed/merged 目錄

使用：python scripts/momo_orders_etl/06_momo_orders_bq_formatter.py

輸入：
- temp/momo/momo_accounting_orders_shop_enriched.csv
- temp/momo/momo_shipping_orders_shop_enriched.csv

輸出：
- data_processed/merged/momo_accounting_orders_bq_formatted_[timestamp].csv
- data_processed/merged/momo_shipping_orders_bq_formatted_[timestamp].csv

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
import logging
import sys
from datetime import datetime
from pathlib import Path

class MomoOrdersBQFormatter:
    def __init__(self):
        # 路徑設定 - 腳本在 scripts/momo_orders_etl/ 目錄下
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parents[1]  # 向上兩層到達專案根目錄
        
        # 檔案路徑
        self.input_dir = self.project_root / "temp" / "momo"
        self.output_dir = self.project_root / "data_processed" / "merged"
        self.logs_dir = self.project_root / "logs"
        
        # 輸入檔案路徑
        self.accounting_file = self.input_dir / "momo_accounting_orders_shop_enriched.csv"
        self.shipping_file = self.input_dir / "momo_shipping_orders_shop_enriched.csv"
        
        # 生成時間戳
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 輸出檔案路徑
        self.accounting_output_file = self.output_dir / f"momo_accounting_orders_bq_formatted_{self.timestamp}.csv"
        self.shipping_output_file = self.output_dir / f"momo_shipping_orders_bq_formatted_{self.timestamp}.csv"
        
        # 確保目錄存在
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
    def setup_logging(self):
        """設定日誌系統"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"momo_orders_bq_formatter_{timestamp}.log"
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
        self.logger.info("=== MOMO 訂單 BigQuery 格式轉換開始 ===")
        
    def check_input_files(self) -> bool:
        """檢查輸入檔案是否存在"""
        self.logger.info("檢查輸入檔案...")
        
        if not self.accounting_file.exists():
            self.logger.error(f"會計訂單檔案不存在: {self.accounting_file}")
            return False
            
        if not self.shipping_file.exists():
            self.logger.error(f"出貨訂單檔案不存在: {self.shipping_file}")
            return False
            
        self.logger.info("✓ 輸入檔案檢查完成")
        return True
    
    def load_order_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """載入訂單資料"""
        self.logger.info("載入訂單資料...")
        
        try:
            # 載入會計訂單（確保特定欄位保持字串格式）
            accounting_df = pd.read_csv(self.accounting_file, encoding='utf-8', dtype=str)
            # 強制將特定欄位轉換為字串，避免小數點
            string_fields = ['product_manufacturer_code', 'product_sku_main', 'product_barcode', 'product_spec']
            for field in string_fields:
                if field in accounting_df.columns:
                    # 先轉換為整數（如果是數值），再轉換為字串
                    if accounting_df[field].dtype in ['int64', 'float64']:
                        accounting_df[field] = accounting_df[field].astype('Int64').astype(str)
                    else:
                        accounting_df[field] = accounting_df[field].astype(str)
            self.logger.info(f"載入會計訂單: {len(accounting_df)} 筆")
            
            # 載入出貨訂單（確保特定欄位保持字串格式）
            shipping_df = pd.read_csv(self.shipping_file, encoding='utf-8', dtype=str)
            # 強制將特定欄位轉換為字串，避免小數點
            for field in string_fields:
                if field in shipping_df.columns:
                    # 先轉換為整數（如果是數值），再轉換為字串
                    if shipping_df[field].dtype in ['int64', 'float64']:
                        shipping_df[field] = shipping_df[field].astype('Int64').astype(str)
                    else:
                        shipping_df[field] = shipping_df[field].astype(str)
            self.logger.info(f"載入出貨訂單: {len(shipping_df)} 筆")
            
            return accounting_df, shipping_df
            
        except Exception as e:
            self.logger.error(f"載入訂單資料時發生錯誤: {e}")
            raise
    
    def convert_to_bigquery_format(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """轉換為 BigQuery 相容格式"""
        self.logger.info(f"開始轉換 {file_type} 為 BigQuery 格式...")
        
        # 複製 DataFrame 避免修改原始資料
        bq_df = df.copy()
        
        # 1. 處理日期和時間欄位
        date_fields = ['order_date', 'actual_shipping_date', 'ship_by_date', 'product_price_date']
        datetime_fields = ['order_transfer_date']
        
        for field in date_fields:
            if field in bq_df.columns:
                self.logger.info(f"處理日期欄位: {field}")
                bq_df[field] = pd.to_datetime(bq_df[field], errors='coerce').dt.strftime('%Y-%m-%d')
                bq_df[field] = bq_df[field].fillna('')
        
        for field in datetime_fields:
            if field in bq_df.columns:
                self.logger.info(f"處理日期時間欄位: {field}")
                bq_df[field] = pd.to_datetime(bq_df[field], errors='coerce').dt.strftime('%Y-%m-%d %H:%M:%S')
                bq_df[field] = bq_df[field].fillna('')
        
        # 2. 處理數值欄位
        # 帳務數字欄位需要保持小數點下兩位
        cost_fields = ['product_cost_untaxed', 'platform_product_cost', 'product_original_price', 'product_cost_from_catalog']
        for field in cost_fields:
            if field in bq_df.columns:
                self.logger.info(f"處理帳務數字欄位: {field}")
                # 轉換為數值，保留小數點下兩位
                bq_df[field] = pd.to_numeric(bq_df[field], errors='coerce').round(2).fillna(0)
                # 確保顯示小數點下兩位
                bq_df[field] = bq_df[field].apply(lambda x: f"{x:.2f}" if pd.notna(x) else "0.00")
        
        # 其他數值欄位
        other_numeric_fields = [
            'quantity', 'product_weight_g', 'product_min_qty', 'product_msrp', 'product_price',
            'product_supplier_price', 'product_list_price'
        ]
        
        for field in other_numeric_fields:
            if field in bq_df.columns:
                self.logger.info(f"處理數值欄位: {field}")
                # 轉換為數值，無法轉換的設為 0
                bq_df[field] = pd.to_numeric(bq_df[field], errors='coerce').fillna(0)
                # 移除小數點後多餘的零
                bq_df[field] = bq_df[field].astype(str).str.replace(r'\.0+$', '', regex=True)
                bq_df[field] = bq_df[field].str.replace('nan', '0')
        
        # 3. 處理布林欄位
        boolean_fields = ['is_abnormal_order', 'shop_is_ad_shopee_ads_enabled']
        
        for field in boolean_fields:
            if field in bq_df.columns:
                self.logger.info(f"處理布林欄位: {field}")
                # 轉換為 BigQuery 布林格式
                bq_df[field] = bq_df[field].astype(str).str.lower()
                bq_df[field] = bq_df[field].map({
                    'true': 'true',
                    'false': 'false',
                    '1': 'true',
                    '0': 'false',
                    'yes': 'true',
                    'no': 'false'
                }).fillna('false')
        
        # 4. 處理字串欄位 - 清理特殊字元
        string_fields = bq_df.select_dtypes(include=['object']).columns.tolist()
        
        for field in string_fields:
            if field not in date_fields + datetime_fields + cost_fields + other_numeric_fields + boolean_fields:
                self.logger.info(f"清理字串欄位: {field}")
                # 移除控制字元，保留基本可列印字元
                bq_df[field] = bq_df[field].astype(str).str.replace(r'[\x00-\x08\x0b\x0c\x0e-\x1f\x7f-\x9f]', '', regex=True)
                # 移除多餘空白
                bq_df[field] = bq_df[field].str.strip()
                # 將 NaN 轉為空字串
                bq_df[field] = bq_df[field].fillna('')
        
        # 5. 添加處理時間戳
        bq_df['bq_processing_timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        # 6. 重新排列欄位順序（將重要欄位放在前面）
        important_fields = [
            'platform', 'order_sn', 'order_date', 'order_sn_main', 'order_line_number',
            'order_sub_sequence', 'order_detail_sequence', 'item_sequence',
            'quantity', 'recipient_name', 'shipping_provider', 'tracking_number',
            'delivery_type', 'order_type', 'actual_shipping_date',
            'order_transfer_date', 'ship_by_date', 'is_abnormal_order',
            'data_source', 'key_for_merge'
        ]
        
        # 商品相關欄位（包括基本商品資訊和詳細資訊）
        product_fields = [col for col in bq_df.columns if col.startswith('product_')]
        
        # 商店詳細資訊欄位
        shop_fields = [col for col in bq_df.columns if col.startswith('shop_')]
        
        # 其他欄位（排除已分類的欄位）
        excluded_fields = set(important_fields + product_fields + shop_fields + ['bq_processing_timestamp'])
        other_fields = [col for col in bq_df.columns if col not in excluded_fields]
        
        # 重新排列欄位順序
        column_order = important_fields + product_fields + shop_fields + other_fields + ['bq_processing_timestamp']
        
        # 只保留實際存在的欄位
        existing_columns = [col for col in column_order if col in bq_df.columns]
        bq_df = bq_df[existing_columns]
        
        self.logger.info(f"{file_type} BigQuery 格式轉換完成，共 {len(bq_df)} 筆，{len(bq_df.columns)} 個欄位")
        
        return bq_df
    
    def save_bigquery_data(self, df: pd.DataFrame, output_file: Path, file_type: str) -> None:
        """儲存 BigQuery 格式資料"""
        self.logger.info(f"儲存 {file_type} BigQuery 格式資料...")
        
        try:
            # 使用 UTF-8 編碼和逗號分隔符
            df.to_csv(output_file, index=False, encoding='utf-8', sep=',')
            self.logger.info(f"✓ {file_type} BigQuery 格式資料已儲存至: {output_file}")
            
            # 顯示檔案大小
            file_size = output_file.stat().st_size / 1024 / 1024  # MB
            self.logger.info(f"  檔案大小: {file_size:.2f} MB")
            
        except Exception as e:
            self.logger.error(f"儲存 {file_type} BigQuery 格式資料時發生錯誤: {e}")
            raise
    
    def generate_summary_report(self, accounting_df: pd.DataFrame, shipping_df: pd.DataFrame,
                              accounting_bq: pd.DataFrame, shipping_bq: pd.DataFrame) -> None:
        """生成處理摘要報告"""
        self.logger.info("生成處理摘要報告...")
        
        # 統計各檔案的資料筆數
        accounting_count = len(accounting_df)
        shipping_count = len(shipping_df)
        accounting_bq_count = len(accounting_bq)
        shipping_bq_count = len(shipping_bq)
        
        # 統計欄位數量
        accounting_columns = len(accounting_df.columns)
        shipping_columns = len(shipping_df.columns)
        accounting_bq_columns = len(accounting_bq.columns)
        shipping_bq_columns = len(shipping_bq.columns)
        
        self.logger.info("=" * 60)
        self.logger.info("BigQuery 格式轉換摘要報告")
        self.logger.info("=" * 60)
        self.logger.info(f"會計訂單檔案:")
        self.logger.info(f"  原始筆數: {accounting_count:,} 筆")
        self.logger.info(f"  轉換後筆數: {accounting_bq_count:,} 筆")
        self.logger.info(f"  原始欄位數: {accounting_columns} 個")
        self.logger.info(f"  轉換後欄位數: {accounting_bq_columns} 個")
        self.logger.info("")
        self.logger.info(f"出貨訂單檔案:")
        self.logger.info(f"  原始筆數: {shipping_count:,} 筆")
        self.logger.info(f"  轉換後筆數: {shipping_bq_count:,} 筆")
        self.logger.info(f"  原始欄位數: {shipping_columns} 個")
        self.logger.info(f"  轉換後欄位數: {shipping_bq_columns} 個")
        self.logger.info("")
        self.logger.info(f"總計:")
        self.logger.info(f"  原始總筆數: {accounting_count + shipping_count:,} 筆")
        self.logger.info(f"  轉換後總筆數: {accounting_bq_count + shipping_bq_count:,} 筆")
        self.logger.info(f"  原始總欄位數: {accounting_columns + shipping_columns} 個")
        self.logger.info(f"  轉換後總欄位數: {accounting_bq_columns + shipping_bq_columns} 個")
        self.logger.info("=" * 60)
    
    def run(self) -> None:
        """執行主要的 BigQuery 格式轉換流程"""
        try:
            self.logger.info("開始執行 MOMO 訂單 BigQuery 格式轉換...")
            
            # 檢查輸入檔案
            if not self.check_input_files():
                return
            
            # 載入訂單資料
            accounting_df, shipping_df = self.load_order_data()
            
            # 轉換會計訂單為 BigQuery 格式
            accounting_bq = self.convert_to_bigquery_format(accounting_df, "會計訂單")
            
            # 轉換出貨訂單為 BigQuery 格式
            shipping_bq = self.convert_to_bigquery_format(shipping_df, "出貨訂單")
            
            # 儲存 BigQuery 格式資料
            self.save_bigquery_data(accounting_bq, self.accounting_output_file, "會計訂單")
            self.save_bigquery_data(shipping_bq, self.shipping_output_file, "出貨訂單")
            
            # 生成摘要報告
            self.generate_summary_report(accounting_df, shipping_df, accounting_bq, shipping_bq)
            
            self.logger.info("✓ MOMO 訂單 BigQuery 格式轉換完成！")
            self.logger.info(f"✓ 會計訂單 BigQuery 格式: {self.accounting_output_file}")
            self.logger.info(f"✓ 出貨訂單 BigQuery 格式: {self.shipping_output_file}")
            
        except Exception as e:
            self.logger.error(f"執行過程中發生錯誤: {e}")
            raise

def main():
    """主函數"""
    formatter = MomoOrdersBQFormatter()
    formatter.run()

if __name__ == "__main__":
    main()
