# scripts/etmall_orders_etl/etmall_batch_processor.py
# -*- coding: utf-8 -*-
"""
東森購物批次處理主腳本

功能：
- 統一執行東森購物的資料處理流程
- 先執行 xlsx 轉換，再執行資料清洗
- 使用 subprocess 執行子腳本，避免導入問題
- 提供完整的資料處理流程

使用：
- python scripts/etmall_orders_etl/etmall_batch_processor.py          # 執行全部
- python scripts/etmall_orders_etl/etmall_batch_processor.py convert  # 只執行 xlsx 轉換
- python scripts/etmall_orders_etl/etmall_batch_processor.py clean    # 只執行資料清洗

輸入：
- data_raw/etmall/*.xlsx 檔案
- data_raw/etmall/*.csv 檔案

輸出：
- data_raw/etmall/*.csv 檔案（xlsx 轉換後）
- data_processed/merged/etmall_orders_cleaned.csv（清洗後）

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path

class EtmallBatchProcessor:
    def __init__(self):
        # 路徑設定 - 腳本在 scripts/etmall_orders_etl/ 目錄下
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parents[1]  # 向上兩層到達專案根目錄
        self.logs_dir = self.project_root / "logs"
        
        # 確保目錄存在
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
        # 腳本路徑 - 同目錄下的其他腳本
        self.convert_script = self.script_dir / "etmall_xlsx_to_csv.py"
        self.clean_script = self.script_dir / "etmall_orders_cleaner.py"
        
    def setup_logging(self):
        """設定批次處理日誌"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"etmall_batch_processor_{timestamp}.log"
        log_path = self.logs_dir / log_filename
        
        # 設定檔案 handler
        file_handler = logging.FileHandler(log_path, encoding='utf-8')
        file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 設定控制台 handler，強制使用 UTF-8 編碼
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
        
        # 設定根 logger
        logging.basicConfig(
            level=logging.INFO,
            handlers=[file_handler, console_handler]
        )
        
        self.logger = logging.getLogger(__name__)
        self.logger.info("=== 東森購物批次處理開始 ===")
        self.logger.info(f"專案根目錄：{self.project_root}")
        self.logger.info(f"腳本目錄：{self.script_dir}")
        
    def run_script(self, script_path, script_name):
        """執行指定的腳本"""
        try:
            if not script_path.exists():
                self.logger.error(f"找不到腳本檔案：{script_path}")
                return False
            
            self.logger.info(f"開始執行 {script_name}...")
            self.logger.info(f"腳本路徑：{script_path}")
            
            # 嘗試不同的編碼方式來處理非 UTF-8 字元
            encodings_to_try = ['utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
            
            for encoding in encodings_to_try:
                try:
                    # 使用 subprocess 執行腳本
                    result = subprocess.run(
                        [sys.executable, str(script_path)],
                        cwd=str(self.project_root),
                        capture_output=True,
                        text=True,
                        encoding=encoding,
                        errors='replace'
                    )
                    
                    # 記錄輸出
                    if result.stdout:
                        self.logger.info(f"{script_name} 輸出：")
                        for line in result.stdout.split('\n'):
                            if line.strip():
                                self.logger.info(f"  {line}")
                    
                    # 記錄錯誤
                    if result.stderr:
                        self.logger.warning(f"{script_name} 錯誤輸出：")
                        for line in result.stderr.split('\n'):
                            if line.strip():
                                self.logger.warning(f"  {line}")
                    
                    # 檢查執行結果
                    if result.returncode == 0:
                        self.logger.info(f"{script_name} 執行成功 (使用編碼: {encoding})")
                        return True
                    else:
                        self.logger.error(f"{script_name} 執行失敗，返回碼：{result.returncode}")
                        return False
                        
                except UnicodeDecodeError:
                    self.logger.warning(f"編碼 {encoding} 解碼失敗，嘗試下一個編碼...")
                    continue
                except Exception as e:
                    self.logger.error(f"執行 {script_name} 時發生錯誤：{e}")
                    return False
            
            # 如果所有編碼都失敗，使用最後的 fallback
            self.logger.warning(f"所有編碼都失敗，使用 fallback 方式執行 {script_name}")
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                encoding='utf-8',
                errors='replace'
            )
            
            # 記錄輸出
            if result.stdout:
                self.logger.info(f"{script_name} 輸出：")
                for line in result.stdout.split('\n'):
                    if line.strip():
                        self.logger.info(f"  {line}")
            
            # 記錄錯誤
            if result.stderr:
                self.logger.warning(f"{script_name} 錯誤輸出：")
                for line in result.stderr.split('\n'):
                    if line.strip():
                        self.logger.warning(f"  {line}")
            
            # 檢查執行結果
            if result.returncode == 0:
                self.logger.info(f"{script_name} 執行成功 (使用 fallback)")
                return True
            else:
                self.logger.error(f"{script_name} 執行失敗，返回碼：{result.returncode}")
                return False
                
        except Exception as e:
            self.logger.error(f"執行 {script_name} 時發生錯誤：{e}")
            return False
    
    def run_xlsx_converter(self):
        """執行 xlsx 轉換"""
        return self.run_script(self.convert_script, "xlsx 轉換器")
    
    def run_orders_cleaner(self):
        """執行訂單資料清洗"""
        return self.run_script(self.clean_script, "訂單資料清洗器")
    
    def run_all(self):
        """執行所有處理作業"""
        self.logger.info("執行完整的東森購物資料處理流程")
        
        results = {
            'convert': False,
            'clean': False
        }
        
        # 執行 xlsx 轉換
        self.logger.info("\n--- 開始 xlsx 轉換 ---")
        results['convert'] = self.run_xlsx_converter()
        
        # 執行訂單資料清洗
        self.logger.info("\n--- 開始訂單資料清洗 ---")
        results['clean'] = self.run_orders_cleaner()
        
        # 總結報告
        self.logger.info("\n=== 批次處理總結 ===")
        success_count = sum(results.values())
        total_count = len(results)
        
        self.logger.info(f"xlsx 轉換：{'成功' if results['convert'] else '失敗'}")
        self.logger.info(f"訂單資料清洗：{'成功' if results['clean'] else '失敗'}")
        self.logger.info(f"整體結果：{success_count}/{total_count} 項作業成功")
        
        return results
    
    def check_prerequisites(self):
        """檢查執行前提條件"""
        self.logger.info("檢查執行前提條件...")
        
        issues = []
        
        # 檢查腳本檔案
        if not self.convert_script.exists():
            issues.append(f"找不到 xlsx 轉換腳本：{self.convert_script}")
        
        if not self.clean_script.exists():
            issues.append(f"找不到訂單清洗腳本：{self.clean_script}")
        
        # 檢查必要目錄
        data_raw_dir = self.project_root / "data_raw" / "etmall"
        if not data_raw_dir.exists():
            issues.append(f"找不到資料目錄：{data_raw_dir}")
        
        # 檢查 mapping 檔案
        mapping_file = self.project_root / "config" / "etmall_fields_mapping.json"
        if not mapping_file.exists():
            issues.append(f"找不到 mapping 檔案：{mapping_file}")
        
        if issues:
            self.logger.error("發現以下問題：")
            for issue in issues:
                self.logger.error(f"  - {issue}")
            return False
        
        self.logger.info("✅ 所有前提條件檢查通過")
        return True
    
    def run(self, mode='all'):
        """執行批次處理"""
        if not self.check_prerequisites():
            self.logger.error("前提條件檢查失敗，停止執行")
            return False
        
        if mode == 'all':
            return self.run_all()
        elif mode == 'convert':
            self.logger.info("只執行 xlsx 轉換")
            return self.run_xlsx_converter()
        elif mode == 'clean':
            self.logger.info("只執行訂單資料清洗")
            return self.run_orders_cleaner()
        else:
            self.logger.error(f"未知的執行模式：{mode}")
            return False

def show_usage():
    """顯示使用說明"""
    print("""
東森購物批次處理器使用說明：

用法：
  python scripts/etmall_orders_etl/etmall_batch_processor.py [模式]

模式選項：
  all      - 執行完整流程（xlsx 轉換 + 資料清洗）
  convert  - 只執行 xlsx 轉換
  clean    - 只執行資料清洗

範例：
  python scripts/etmall_orders_etl/etmall_batch_processor.py          # 執行全部
  python scripts/etmall_orders_etl/etmall_batch_processor.py convert  # 只轉換 xlsx
  python scripts/etmall_orders_etl/etmall_batch_processor.py clean    # 只清洗資料

流程說明：
  1. xlsx 轉換：將 data_raw/etmall/*.xlsx 轉換為 *.csv
  2. 資料清洗：讀取 CSV 檔案，按 mapping 清洗並輸出到 data_processed/merged/
""")

def main():
    """主程式入口點"""
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        if mode in ['-h', '--help', 'help']:
            show_usage()
            return
        elif mode not in ['all', 'convert', 'clean']:
            print(f"錯誤：未知的執行模式 '{mode}'")
            show_usage()
            return
    else:
        mode = 'all'
    
    processor = EtmallBatchProcessor()
    success = processor.run(mode)
    
    if success:
        print("✅ 批次處理完成")
    else:
        print("❌ 批次處理失敗")
        sys.exit(1)

if __name__ == "__main__":
    main() 