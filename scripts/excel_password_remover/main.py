# scripts/excel_password_remover/main.py
# -*- coding: utf-8 -*-
"""
自動解壓縮＋多帳號 Excel 密碼移除＋完整 log
用途：
    - 支援多平台/多帳號/多密碼。
    - 將所有資料處理過程紀錄在 log。
    - 檢查密碼檔未處理檔案提示。
    - 批次處理 data_raw/shopee 下的所有檔案（xlsx 和 csv）
"""

from pathlib import Path
from remover import remove_password
from utils import ensure_dir, extract_zip_files, load_passwords_json
import datetime
import shutil
import sys
import pandas as pd
from typing import List, Dict, Any

def main() -> None:
    # 根目錄推算（從任何路徑啟動都可！）
    project_root: Path = Path(__file__).resolve().parents[2]
    raw_dir: Path = project_root / "data_raw"
    temp_dir: Path = project_root / "temp"
    log_dir: Path = project_root / "logs"
    passwords_json: Path = project_root / "config" / "ec_shops_universal_passwords.json"

    ensure_dir(temp_dir)
    ensure_dir(log_dir)

    # 清空 temp 目錄所有平台
    for platform_dir in temp_dir.iterdir():
        if platform_dir.is_dir():
            for file in platform_dir.glob("*"):
                if file.is_file():
                    file.unlink()
                elif file.is_dir():
                    shutil.rmtree(file)

    # 清空所有 log 檔案
    for file in log_dir.glob("execution_log_*.txt"):
        try:
            file.unlink()
        except PermissionError:
            pass
    log_path: Path = log_dir / f"execution_log_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt"

    # 讀密碼設定
    data: List[Dict[str, Any]] = load_passwords_json(passwords_json)
    accounts: Dict[str, Dict[str, str]] = {}
    for item in data:
        shop_name = item.get("shop_name")
        shop_account = item.get("shop_account")
        password = item.get("report_download_password")
        if shop_name and password:
            accounts[shop_name] = {
                "account": shop_account or "",
                "password": password
            }

    log_lines: List[str] = []
    processed_files: List[str] = []

    # 遞迴掃描 data_raw 所有子目錄
    for file_path in raw_dir.rglob("*"):
        if not file_path.is_file():
            continue
        
        filename = file_path.name
        if filename.startswith("."):
            continue
            
        # 支援的檔案格式
        if not filename.lower().endswith(('.xlsx', '.xls', '.csv', '.zip')):
            continue

        # 確定平台和輸出目錄
        platform = file_path.parent.name
        platform_temp_dir = temp_dir / platform
        ensure_dir(platform_temp_dir)
        
        # 壓縮檔處理
        if filename.lower().endswith('.zip'):
            try:
                extract_zip_files(str(file_path), str(platform_temp_dir))
                log_lines.append(f"解壓縮成功: {platform}/{filename}")
                
                # 處理解壓後的檔案
                for extracted_file in platform_temp_dir.glob("*"):
                    if extracted_file.is_file() and extracted_file.name.lower().endswith(('.xlsx', '.xls', '.csv')):
                        extracted_filename = extracted_file.name
                        is_excel = extracted_filename.lower().endswith(('.xlsx', '.xls'))
                        
                        # 尋找匹配的帳號密碼
                        matched_account = None
                        for shop_name, info in accounts.items():
                            if shop_name in extracted_filename or (info["account"] and info["account"] in extracted_filename):
                                matched_account = {"name": shop_name, **info}
                                break
                        
                        if matched_account and is_excel:
                            # Excel 密碼移除
                            try:
                                temp_excel_path = platform_temp_dir / f"temp_{extracted_filename}"
                                remove_password(str(extracted_file), str(temp_excel_path), matched_account["password"])
                                log_lines.append(f'{matched_account["name"]} 解壓後 Excel 密碼移除成功: {platform}/{extracted_filename}')
                                
                                # 轉換 CSV
                                try:
                                    csv_filename = extracted_filename.rsplit('.', 1)[0] + '.csv'
                                    csv_path = platform_temp_dir / csv_filename
                                    
                                    df = pd.read_excel(temp_excel_path)
                                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                                    log_lines.append(f'{matched_account["name"]} 解壓後轉換 CSV 成功: {platform}/{csv_filename}')
                                    processed_files.append(f"{platform}/{csv_filename}")
                                    
                                    # 刪除臨時 Excel 和原檔
                                    temp_excel_path.unlink()
                                    extracted_file.unlink()
                                    
                                except Exception as csv_e:
                                    log_lines.append(f'{matched_account["name"]} 解壓後轉換 CSV 失敗: {platform}/{extracted_filename} - {csv_e}')
                                    processed_files.append(f"{platform}/{extracted_filename}")
                                    
                            except Exception as e:
                                log_lines.append(f'{matched_account["name"]} 解壓後 Excel 密碼移除失敗: {platform}/{extracted_filename} - {e}')
                                processed_files.append(f"{platform}/{extracted_filename}")
                        else:
                            # 非 Excel 或無密碼，直接保留
                            processed_files.append(f"{platform}/{extracted_filename}")
                    else:
                        processed_files.append(f"{platform}/{extracted_file.name}")
                        
            except Exception as e:
                log_lines.append(f"解壓縮失敗 {platform}/{filename}: {e}")
            continue

        # 檔案處理
        output_path = platform_temp_dir / filename
        is_excel_file = filename.lower().endswith(('.xlsx', '.xls'))
        
        # 尋找匹配的帳號密碼
        matched_account = None
        for shop_name, info in accounts.items():
            if shop_name in filename or (info["account"] and info["account"] in filename):
                matched_account = {"name": shop_name, **info}
                break
        
        if matched_account and is_excel_file:
            # Excel 密碼移除
            try:
                remove_password(str(file_path), str(output_path), matched_account["password"])
                log_lines.append(f'{matched_account["name"]} Excel 密碼移除成功: {platform}/{filename}')
                
                # 轉換 CSV
                try:
                    csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                    csv_output_path = platform_temp_dir / csv_filename
                    
                    df = pd.read_excel(output_path)
                    df.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
                    log_lines.append(f'{matched_account["name"]} 轉換 CSV 成功: {platform}/{csv_filename}')
                    processed_files.append(f"{platform}/{csv_filename}")
                    
                    # 刪除 Excel
                    output_path.unlink()
                    
                except Exception as csv_e:
                    log_lines.append(f'{matched_account["name"]} 轉換 CSV 失敗: {platform}/{filename} - {csv_e}')
                    processed_files.append(f"{platform}/{filename}")
                    
            except Exception as e:
                log_lines.append(f'{matched_account["name"]} Excel 密碼移除失敗: {platform}/{filename} - {e}')
                # 失敗時直接複製
                try:
                    shutil.copyfile(file_path, output_path)
                    log_lines.append(f"{platform}/{filename} 直接複製")
                    processed_files.append(f"{platform}/{filename}")
                except Exception as copy_e:
                    log_lines.append(f"{platform}/{filename} 複製失敗: {copy_e}")
        else:
            # 直接複製
            try:
                shutil.copyfile(file_path, output_path)
                log_lines.append(f"{platform}/{filename} 直接複製")
                processed_files.append(f"{platform}/{filename}")
            except Exception as copy_e:
                log_lines.append(f"{platform}/{filename} 複製失敗: {copy_e}")

    # 輸出摘要
    log_lines.append(f"\n處理摘要:")
    log_lines.append(f"   - 總共處理 {len(processed_files)} 個檔案")
    log_lines.append(f"   - 處理的檔案: {', '.join(processed_files)}")
    log_lines.append(f"   - 輸出目錄: {temp_dir}")

    # 寫入 log
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_lines))
    print(f"\n執行 log 已產生：{log_path}")
    print(f"處理摘要: 總共處理 {len(processed_files)} 個檔案")
    print(f"檔案已輸出到: {temp_dir}")

if __name__ == "__main__":
    main()
    if '--no-wait' not in sys.argv:
        try:
            input("\n✅ 執行完畢，請按 Enter 關閉視窗...")
        except:
            pass