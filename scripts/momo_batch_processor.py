# scripts/momo_batch_processor_simple.py
# -*- coding: utf-8 -*-
"""
MOMO 批次處理主腳本 (簡化版)

功能：
- 統一執行 MOMO 的兩個清理腳本
- 使用 subprocess 執行子腳本，避免導入問題
- 提供選擇性執行選項

使用：
- python scripts/momo_batch_processor_simple.py          # 執行全部
- python scripts/momo_batch_processor_simple.py shipping # 只執行出貨管理
- python scripts/momo_batch_processor_simple.py accounting # 只執行帳務對帳
"""

import sys
import logging
import subprocess
from datetime import datetime
from pathlib import Path

class MomoBatchProcessor:
    def __init__(self):
        # 路徑設定
        self.script_dir = Path(__file__).parent
        self.project_root = self.script_dir.parent
        self.logs_dir = self.project_root / "logs"
        
        # 確保目錄存在
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        
        # 設定日誌
        self.setup_logging()
        
        # 腳本路徑
        self.shipping_script = self.script_dir / "momo_shipping_cleaner.py"
        self.accounting_script = self.script_dir / "momo_accounting_cleaner.py"
        
    def setup_logging(self):
        """設定批次處理日誌"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"momo_batch_processor_{timestamp}.log"
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
        self.logger.info("=== MOMO 批次處理開始 ===")
        
    def run_script(self, script_path, script_name):
        """執行指定的腳本"""
        try:
            if not script_path.exists():
                self.logger.error(f"找不到腳本檔案：{script_path}")
                return False
            
            self.logger.info(f"開始執行 {script_name}...")
            
            # 使用 subprocess 執行腳本
            result = subprocess.run(
                [sys.executable, str(script_path)],
                cwd=str(self.project_root),
                capture_output=True,
                text=True,
                encoding='utf-8'
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
                self.logger.info(f"{script_name} 執行成功")
                return True
            else:
                self.logger.error(f"{script_name} 執行失敗，返回碼：{result.returncode}")
                return False
                
        except Exception as e:
            self.logger.error(f"執行 {script_name} 時發生錯誤：{e}")
            return False
    
    def run_shipping_cleaner(self):
        """執行出貨管理清理"""
        return self.run_script(self.shipping_script, "出貨管理清理 (A1102 系列)")
    
    def run_accounting_cleaner(self):
        """執行帳務對帳清理"""
        return self.run_script(self.accounting_script, "帳務對帳清理 (C1105 系列)")
    
    def run_all(self):
        """執行所有清理作業"""
        self.logger.info("執行完整的 MOMO 資料清理流程")
        
        results = {
            'shipping': False,
            'accounting': False
        }
        
        # 執行出貨管理清理
        results['shipping'] = self.run_shipping_cleaner()
        
        # 執行帳務對帳清理
        results['accounting'] = self.run_accounting_cleaner()
        
        # 總結報告
        self.logger.info("\n=== 批次處理總結 ===")
        success_count = sum(results.values())
        total_count = len(results)
        
        self.logger.info(f"出貨管理清理：{'✅ 成功' if results['shipping'] else '❌ 失敗'}")
        self.logger.info(f"帳務對帳清理：{'✅ 成功' if results['accounting'] else '❌ 失敗'}")
        self.logger.info(f"整體結果：{success_count}/{total_count} 項作業成功")
        
        return results
    
    def run(self, mode='all'):
        """根據模式執行相應的清理作業"""
        try:
            if mode == 'shipping':
                self.logger.info("模式：僅執行出貨管理清理")
                success = self.run_shipping_cleaner()
                if success:
                    self.logger.info("✅ 出貨管理清理成功完成")
                else:
                    self.logger.error("❌ 出貨管理清理失敗")
                return success
                
            elif mode == 'accounting':
                self.logger.info("模式：僅執行帳務對帳清理")
                success = self.run_accounting_cleaner()
                if success:
                    self.logger.info("✅ 帳務對帳清理成功完成")
                else:
                    self.logger.error("❌ 帳務對帳清理失敗")
                return success
                
            elif mode == 'all':
                self.logger.info("模式：執行完整清理流程")
                results = self.run_all()
                success = all(results.values())
                if success:
                    self.logger.info("✅ 所有清理作業成功完成")
                else:
                    failed_tasks = [task for task, result in results.items() if not result]
                    self.logger.warning(f"⚠️ 部分作業失敗：{failed_tasks}")
                return success
                
            else:
                self.logger.error(f"未知的執行模式：{mode}")
                self.logger.info("可用模式：all, shipping, accounting")
                return False
                
        except Exception as e:
            self.logger.error(f"批次處理執行失敗：{e}")
            return False
        finally:
            self.logger.info("=== MOMO 批次處理結束 ===")

def show_usage():
    """顯示使用說明"""
    print("""
MOMO 批次處理腳本使用說明：

用法：
  python scripts/momo_batch_processor_simple.py [模式]

模式選項：
  all         執行完整清理流程 (預設)
              - 出貨管理清理 (A1102_2, A1102_3)
              - 帳務對帳清理 (C1105)
              
  shipping    僅執行出貨管理清理
              - 處理 A1102_2_*.csv 和 A1102_3_*.csv
              - 輸出：momo_shipping_orders_cleaned.csv
              
  accounting  僅執行帳務對帳清理
              - 處理 C1105_*.csv
              - 輸出：momo_accounting_orders_cleaned.csv

範例：
  python scripts/momo_batch_processor_simple.py
  python scripts/momo_batch_processor_simple.py all
  python scripts/momo_batch_processor_simple.py shipping
  python scripts/momo_batch_processor_simple.py accounting
""")

def main():
    """主函式"""
    # 解析命令列參數
    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()
        
        # 檢查是否為幫助請求
        if mode in ['help', '-h', '--help', '?']:
            show_usage()
            return
        
        # 驗證模式
        if mode not in ['all', 'shipping', 'accounting']:
            print(f"❌ 錯誤：未知的模式 '{mode}'")
            show_usage()
            return
    else:
        mode = 'all'  # 預設模式
    
    # 執行批次處理
    processor = MomoBatchProcessor()
    success = processor.run(mode)
    
    # 設定退出碼
    exit_code = 0 if success else 1
    sys.exit(exit_code)

if __name__ == "__main__":
    main()