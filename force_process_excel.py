import pandas as pd
from pathlib import Path
import shutil

# 強制處理殘留的 Excel 檔案
data_raw_dir = Path("ec-data-pipeline/data_raw/etmall")
backup_dir = data_raw_dir / "backup"

# 確保 backup 目錄存在
backup_dir.mkdir(exist_ok=True)

# 尋找所有 Excel 檔案
excel_files = []
for pattern in ["*.xls", "*.xlsx"]:
    files = list(data_raw_dir.glob(pattern))
    excel_files.extend(files)

print(f"找到 {len(excel_files)} 個 Excel 檔案需要處理")

for excel_file in excel_files:
    try:
        print(f"處理檔案：{excel_file.name}")
        
        # 讀取 Excel 內容
        if excel_file.suffix.lower() == '.xlsx':
            df = pd.read_excel(excel_file, engine='openpyxl')
        else:
            df = pd.read_excel(excel_file, engine='xlrd')
        
        print(f"  欄位數：{len(df.columns)}")
        print(f"  資料行數：{len(df)}")
        
        # 生成 CSV 檔名
        csv_filename = excel_file.stem + '.csv'
        csv_path = data_raw_dir / csv_filename
        
        # 轉換為 CSV
        df.to_csv(csv_path, index=False, encoding='utf-8-sig', na_rep='')
        print(f"  已轉換為 CSV：{csv_filename}")
        
        # 移動原始 Excel 檔案到 backup
        backup_file_path = backup_dir / excel_file.name
        if backup_file_path.exists():
            # 如果 backup 中已有同名檔案，加上時間戳
            import datetime
            timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_filename = f"{excel_file.stem}_{timestamp}{excel_file.suffix}"
            backup_file_path = backup_dir / backup_filename
        
        shutil.move(str(excel_file), str(backup_file_path))
        print(f"  已移動到 backup：{backup_file_path.name}")
        
    except Exception as e:
        print(f"  處理檔案 {excel_file.name} 時發生錯誤：{e}")

print("\n處理完成！")
