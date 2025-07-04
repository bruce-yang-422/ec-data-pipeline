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
from typing import List, Set, Dict, Any

def main() -> None:
    # 根目錄推算（從任何路徑啟動都可！）
    project_root: Path = Path(__file__).resolve().parents[2]
    raw_dir: Path = project_root / "data_raw" / "shopee"
    temp_dir: Path = project_root / "temp" / "shopee"
    log_dir: Path = project_root / "logs"
    passwords_json: Path = project_root / "config" / "ec_shops_universal_passwords.json"

    ensure_dir(temp_dir)
    ensure_dir(log_dir)

    # 清空 temp/shopee 目錄
    for file in temp_dir.glob("*"):
        if file.is_file():
            file.unlink()
        elif file.is_dir():
            shutil.rmtree(file)

    # 只清空 excel_password_remover 相關的 log 檔案
    for file in log_dir.glob("execution_log_*.txt"):
        try:
            file.unlink()
        except PermissionError:
            # 如果檔案被使用中，跳過不刪除
            pass
    log_path: Path = log_dir / f"execution_log_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt"

    # 讀密碼
    data: List[Dict[str, Any]] = load_passwords_json(passwords_json)
    # 以 shop_account 為 key
    accounts: Dict[str, str] = {}
    for item in data:
        shop_account = item.get("shop_account")
        shop_name = item.get("shop_name")
        if shop_account and shop_name:
            accounts[shop_account] = shop_name

    log_lines: List[str] = []
    processed_accounts: Set[str] = set()
    processed_files: List[str] = []

    # 掃描處理所有檔案
    for input_path in raw_dir.iterdir():
        if not input_path.is_file():
            continue
        filename: str = input_path.name
        # 跳過隱藏檔案
        if filename.startswith("."):
            continue

        # 自動處理壓縮檔
        if filename.lower().endswith('.zip'):
            try:
                extract_zip_files(str(input_path), str(temp_dir))
                log_lines.append(f"解壓縮成功: {filename}")
                # 解壓縮後，處理解壓縮出來的檔案
                for extracted_file in temp_dir.glob("*"):
                    if extracted_file.is_file():
                        processed_files.append(extracted_file.name)
            except Exception as e:
                log_lines.append(f"解壓縮失敗 {filename}: {e}")
            continue

        # 處理 Excel 和 CSV 檔案
        if filename.lower().endswith(('.xlsx', '.xls', '.csv')):
            output_path: Path = temp_dir / filename
            
            # 檢查是否有對應的密碼
            matched_accounts: List[str] = [account for account in accounts if account and account in filename]
            
            # 修正：明確檢查 Excel 檔案格式
            is_excel_file = filename.lower().endswith(('.xlsx', '.xls'))
            
            if matched_accounts and is_excel_file:
                # 有密碼且是 Excel 檔案，嘗試移除密碼
                account: str = matched_accounts[0]
                name: str = accounts[account]
                password: str | None = None
                for item in data:
                    if item.get("shop_account") == account:
                        password = item.get("report_download_password")
                        break
                
                if password:
                    try:
                        remove_password(str(input_path), str(output_path), password)
                        log_lines.append(f"{name} ({account}) Excel 密碼移除成功: {filename}")
                        processed_accounts.add(account)
                        processed_files.append(filename)
                        
                        # 轉換為 CSV
                        try:
                            csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                            csv_output_path = temp_dir / csv_filename
                            
                            # 讀取 Excel 並轉換為 CSV
                            df = pd.read_excel(output_path)
                            df.to_csv(csv_output_path, index=False, encoding='utf-8-sig')
                            log_lines.append(f"{name} ({account}) 轉換 CSV 成功: {csv_filename}")
                            processed_files.append(csv_filename)
                            
                            # 刪除 Excel 檔案，只保留 CSV
                            output_path.unlink()
                            processed_files.remove(filename)
                            
                        except Exception as csv_e:
                            log_lines.append(f"{name} ({account}) 轉換 CSV 失敗: {filename} - {csv_e}")
                            
                    except Exception as e:
                        log_lines.append(f"{name} ({account}) Excel 密碼移除失敗: {filename} - {e}")
                        # 如果密碼移除失敗，嘗試直接複製
                        try:
                            shutil.copyfile(input_path, output_path)
                            log_lines.append(f"{filename} 直接複製到 temp/shopee")
                            processed_files.append(filename)
                        except Exception as copy_e:
                            log_lines.append(f"{filename} 複製失敗: {copy_e}")
                else:
                    log_lines.append(f"沒有找到 {account} 的密碼，直接複製: {filename}")
                    try:
                        shutil.copyfile(input_path, output_path)
                        log_lines.append(f"{filename} 直接複製到 temp/shopee")
                        processed_files.append(filename)
                    except Exception as copy_e:
                        log_lines.append(f"{filename} 複製失敗: {copy_e}")
            else:
                # 沒有密碼或不是 Excel 檔案，直接複製
                try:
                    shutil.copyfile(input_path, output_path)
                    log_lines.append(f"{filename} 直接複製到 temp/shopee")
                    processed_files.append(filename)
                except Exception as copy_e:
                    log_lines.append(f"{filename} 複製失敗: {copy_e}")

    # 檢查哪些帳號沒被處理
    for account, name in accounts.items():
        if account and account not in processed_accounts:
            log_lines.append(f"{name} ({account}) 在密碼表中，但未找到對應檔案")

    # 輸出處理結果摘要
    log_lines.append(f"\n處理摘要:")
    log_lines.append(f"   - 總共處理 {len(processed_files)} 個檔案")
    log_lines.append(f"   - 處理的檔案: {', '.join(processed_files)}")
    log_lines.append(f"   - 輸出目錄: {temp_dir}")

    # 輸出 log
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