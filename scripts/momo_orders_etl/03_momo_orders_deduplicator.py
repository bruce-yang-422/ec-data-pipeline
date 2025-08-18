#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Momo 訂單去重腳本
針對 data_processed/merged/ 下的兩個 Momo 訂單檔案分別進行去重處理
使用 order_sn 作為去重的 key
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime
import sys

# 設定日誌
def setup_logging():
    """設定日誌配置"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"momo_orders_deduplicator_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

class MomoOrdersDeduplicator:
    def __init__(self):
        self.project_root = Path(__file__).parent.parent.parent
        self.input_dir = self.project_root / "temp" / "momo"
        self.output_dir = self.project_root / "data_processed" / "merged"
        self.logger = setup_logging()
        
        # 輸入檔案路徑
        self.accounting_file = self.input_dir / "momo_accounting_orders_cleaned.csv"
        self.shipping_file = self.input_dir / "momo_shipping_orders_cleaned.csv"
        
        # 輸出檔案路徑
        self.accounting_deduplicated_file = self.output_dir / "momo_accounting_orders_deduplicated.csv"
        self.shipping_deduplicated_file = self.output_dir / "momo_shipping_orders_deduplicated.csv"
        
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
    
    def load_data(self) -> tuple[pd.DataFrame, pd.DataFrame]:
        """載入兩個訂單檔案"""
        self.logger.info("載入訂單資料...")
        
        try:
            # 載入會計訂單
            accounting_df = pd.read_csv(self.accounting_file, encoding='utf-8')
            self.logger.info(f"載入會計訂單: {len(accounting_df)} 筆")
            
            # 載入出貨訂單
            shipping_df = pd.read_csv(self.shipping_file, encoding='utf-8')
            self.logger.info(f"載入出貨訂單: {len(shipping_df)} 筆")
            
            return accounting_df, shipping_df
            
        except Exception as e:
            self.logger.error(f"載入資料時發生錯誤: {e}")
            raise
    
    def deduplicate_dataframe(self, df: pd.DataFrame, file_type: str) -> pd.DataFrame:
        """對單個 DataFrame 進行去重處理"""
        self.logger.info(f"開始處理 {file_type} 檔案的去重...")
        
        # 檢查 order_sn 欄位是否存在
        if 'order_sn' not in df.columns:
            self.logger.error(f"找不到 order_sn 欄位，無法進行去重: {file_type}")
            raise ValueError(f"找不到 order_sn 欄位: {file_type}")
        
        # 檢查重複的 order_sn
        duplicate_counts = df['order_sn'].value_counts()
        duplicates = duplicate_counts[duplicate_counts > 1]
        
        if not duplicates.empty:
            self.logger.info(f"{file_type} 發現 {len(duplicates)} 個重複的 order_sn:")
            for order_sn, count in duplicates.head(10).items():
                self.logger.info(f"  {order_sn}: {count} 筆")
            if len(duplicates) > 10:
                self.logger.info(f"  ... 還有 {len(duplicates) - 10} 個重複的 order_sn")
        else:
            self.logger.info(f"{file_type} 沒有發現重複的 order_sn")
        
        # 進行去重，保留最後一筆（最新的）
        before_dedup = len(df)
        df_deduplicated = df.drop_duplicates(subset=['order_sn'], keep='last')
        after_dedup = len(df_deduplicated)
        
        self.logger.info(f"{file_type} 去重完成: {before_dedup} -> {after_dedup} 筆")
        self.logger.info(f"{file_type} 移除重複資料: {before_dedup - after_dedup} 筆")
        
        return df_deduplicated
    
    def save_deduplicated_data(self, df: pd.DataFrame, output_file: Path, file_type: str) -> None:
        """儲存去重後的資料"""
        self.logger.info(f"儲存 {file_type} 去重後的資料...")
        
        try:
            df.to_csv(output_file, index=False, encoding='utf-8')
            self.logger.info(f"✓ {file_type} 資料已儲存至: {output_file}")
            
        except Exception as e:
            self.logger.error(f"儲存 {file_type} 資料時發生錯誤: {e}")
            raise
    
    def generate_summary_report(self, accounting_df: pd.DataFrame, shipping_df: pd.DataFrame, 
                              accounting_deduplicated: pd.DataFrame, shipping_deduplicated: pd.DataFrame) -> None:
        """生成處理摘要報告"""
        self.logger.info("生成處理摘要報告...")
        
        # 統計各檔案的資料筆數
        accounting_count = len(accounting_df)
        shipping_count = len(shipping_df)
        accounting_dedup_count = len(accounting_deduplicated)
        shipping_dedup_count = len(shipping_deduplicated)
        
        # 計算去重效果
        accounting_duplicates_removed = accounting_count - accounting_dedup_count
        shipping_duplicates_removed = shipping_count - shipping_dedup_count
        total_duplicates_removed = accounting_duplicates_removed + shipping_duplicates_removed
        
        self.logger.info("=" * 60)
        self.logger.info("處理摘要報告")
        self.logger.info("=" * 60)
        self.logger.info(f"會計訂單檔案:")
        self.logger.info(f"  原始筆數: {accounting_count:,} 筆")
        self.logger.info(f"  去重後筆數: {accounting_dedup_count:,} 筆")
        self.logger.info(f"  移除重複: {accounting_duplicates_removed:,} 筆")
        self.logger.info("")
        self.logger.info(f"出貨訂單檔案:")
        self.logger.info(f"  原始筆數: {shipping_count:,} 筆")
        self.logger.info(f"  去重後筆數: {shipping_dedup_count:,} 筆")
        self.logger.info(f"  移除重複: {shipping_duplicates_removed:,} 筆")
        self.logger.info("")
        self.logger.info(f"總計:")
        self.logger.info(f"  原始總筆數: {accounting_count + shipping_count:,} 筆")
        self.logger.info(f"  去重後總筆數: {accounting_dedup_count + shipping_dedup_count:,} 筆")
        self.logger.info(f"  總移除重複: {total_duplicates_removed:,} 筆")
        self.logger.info("=" * 60)
    
    def run(self) -> None:
        """執行主要的去重流程"""
        try:
            self.logger.info("開始執行 Momo 訂單去重處理...")
            
            # 檢查輸入檔案
            if not self.check_input_files():
                return
            
            # 載入資料
            accounting_df, shipping_df = self.load_data()
            
            # 分別對兩個檔案進行去重
            accounting_deduplicated = self.deduplicate_dataframe(accounting_df, "會計訂單")
            shipping_deduplicated = self.deduplicate_dataframe(shipping_df, "出貨訂單")
            
            # 儲存去重後的結果
            self.save_deduplicated_data(accounting_deduplicated, self.accounting_deduplicated_file, "會計訂單")
            self.save_deduplicated_data(shipping_deduplicated, self.shipping_deduplicated_file, "出貨訂單")
            
            # 生成摘要報告
            self.generate_summary_report(accounting_df, shipping_df, accounting_deduplicated, shipping_deduplicated)
            
            self.logger.info("✓ Momo 訂單去重處理完成！")
            self.logger.info(f"✓ 會計訂單去重結果: {self.accounting_deduplicated_file}")
            self.logger.info(f"✓ 出貨訂單去重結果: {self.shipping_deduplicated_file}")
            
        except Exception as e:
            self.logger.error(f"執行過程中發生錯誤: {e}")
            raise

def main():
    """主函數"""
    deduplicator = MomoOrdersDeduplicator()
    deduplicator.run()

if __name__ == "__main__":
    main()
