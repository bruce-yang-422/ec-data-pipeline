# scripts/etmall_orders_etl/etmall_xlsx_to_csv.py
"""
東森購物訂單資料轉換工具

功能：
- 將東森購物的 Excel 訂單報表轉換為 CSV 格式
- 支援多種編碼格式的 Excel 檔案讀取
- 自動處理資料型態轉換
- 批次處理 data_raw/etmall 資料夾下的所有 xlsx 檔案

輸入：data_raw/etmall/*.xlsx 檔案
輸出：同路徑的 CSV 檔案（副檔名改為 .csv）

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
from pathlib import Path
import sys

def convert_xlsx_to_csv(input_path: Path):
    """將單一 xlsx 檔案轉換為 csv"""
    try:
        # 建立輸出路徑（副檔名改為 .csv）
        output_path = input_path.with_suffix('.csv')
        
        print(f'讀取：{input_path}')
        df = pd.read_excel(input_path, dtype=str)
        print(f'共 {len(df)} 筆資料，{len(df.columns)} 欄')
        
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        print(f'已輸出 CSV：{output_path}')
        return True
        
    except Exception as e:
        print(f'轉換失敗：{input_path} - {e}', file=sys.stderr)
        return False

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]  # 向上兩層到達專案根目錄
    
    # 設定輸入目錄
    input_dir = project_root / 'data_raw' / 'etmall'
    
    # 檢查目錄是否存在
    if not input_dir.exists():
        print(f'錯誤：找不到目錄 {input_dir}', file=sys.stderr)
        sys.exit(1)
    
    # 搜尋所有 xlsx 檔案
    xlsx_files = list(input_dir.glob('*.xlsx'))
    
    if not xlsx_files:
        print(f'在 {input_dir} 目錄下沒有找到任何 xlsx 檔案')
        return
    
    print(f'找到 {len(xlsx_files)} 個 xlsx 檔案：')
    for file in xlsx_files:
        print(f'  - {file.name}')
    
    print('\n開始轉換...')
    
    success_count = 0
    total_count = len(xlsx_files)
    
    for xlsx_file in xlsx_files:
        if convert_xlsx_to_csv(xlsx_file):
            success_count += 1
        print()  # 空行分隔
    
    print(f'轉換完成：{success_count}/{total_count} 個檔案成功轉換')

if __name__ == '__main__':
    main() 