# scripts/excel_password_remover/main.py
# -*- coding: utf-8 -*-
"""
自動解壓縮＋多帳號 Excel 密碼移除＋完整 log ＋ 資料清理
用途：
    - 支援多平台/多帳號/多密碼。
    - 支援 ZIP 和 RAR 檔案解壓縮（含密碼保護）。
    - 將所有資料處理過程紀錄在 log。
    - 檢查密碼檔未處理檔案提示。
    - 批次處理 data_raw/ 下各平台的所有檔案（xlsx、csv、zip、rar）
    - 自動清理資料中的空格和換行符號
流程：
    1. 先解壓縮所有 ZIP/RAR 檔案到 temp/平台名稱（支援密碼）
    2. 移除所有 Excel 檔案密碼
    3. 轉換所有 Excel 檔案為 CSV
    4. 清理 CSV 資料中的空格和換行符號
    5. 刪除 Excel 檔案，只保留 CSV

輸入：
- data_raw/ 目錄下的各平台檔案（支援 .zip、.rar、.xlsx、.xls、.csv）
- config/ec_shops_universal_passwords.json（包含各平台密碼）

輸出：
- temp/ 目錄下的處理後檔案
- logs/execution_log_*.txt

支援的平台密碼：
- etmall: 東森購物密碼
- momo: MOMO購物中心密碼
- shopee: 蝦皮密碼
- pchome: PC購物中心密碼
- yahoo: Yahoo購物中心密碼

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

from pathlib import Path
from remover import remove_password
from utils import ensure_dir, extract_archive_files, batch_extract_archives, load_passwords_json, get_password_for_platform
import datetime
import shutil
import sys
import pandas as pd
import numpy as np
from typing import List, Dict, Any
import re

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    清理 DataFrame 中的空格和換行符號
    - 移除字串欄位前後空格
    - 移除字串中的換行符號、回車符號
    - 移除多餘空格（多個連續空格變成單一空格）
    """
    cleaned_df = df.copy()
    
    for col in cleaned_df.columns:
        # 處理字串型欄位
        if cleaned_df[col].dtype == 'object':
            # 轉換為字串並處理 NaN
            cleaned_df[col] = cleaned_df[col].astype(str)
            
            # 將 'nan' 字串轉回 NaN
            cleaned_df[col] = cleaned_df[col].replace('nan', np.nan)
            
            # 只對非 NaN 的值進行清理
            mask = cleaned_df[col].notna()
            if mask.any():
                # 移除換行符號和回車符號
                cleaned_df.loc[mask, col] = cleaned_df.loc[mask, col].str.replace(r'[\r\n]+', ' ', regex=True)
                
                # 移除多餘空格（多個連續空格變成單一空格）
                cleaned_df.loc[mask, col] = cleaned_df.loc[mask, col].str.replace(r'\s+', ' ', regex=True)
                
                # 移除前後空格
                cleaned_df.loc[mask, col] = cleaned_df.loc[mask, col].str.strip()
                
                # 如果清理後變成空字串，轉為 NaN
                cleaned_df.loc[mask & (cleaned_df[col] == ''), col] = np.nan
    
    return cleaned_df

def clean_csv_file(file_path: Path, log_lines: List[str]) -> bool:
    """
    清理 CSV 檔案中的空格和換行符號
    返回清理是否成功
    """
    try:
        # 讀取 CSV
        df = pd.read_csv(file_path, encoding='utf-8-sig')
        original_shape = df.shape
        
        # 清理資料
        cleaned_df = clean_dataframe(df)
        
        # 計算清理統計
        changes_count = 0
        for col in df.columns:
            if df[col].dtype == 'object':
                # 比較原始和清理後的資料
                original_values = df[col].fillna('').astype(str)
                cleaned_values = cleaned_df[col].fillna('').astype(str)
                changes_count += (original_values != cleaned_values).sum()
        
        # 儲存清理後的檔案
        cleaned_df.to_csv(file_path, index=False, encoding='utf-8-sig')
        
        platform = file_path.parent.name
        filename = file_path.name
        log_lines.append(f"資料清理成功: {platform}/{filename} (形狀: {original_shape}, 清理: {changes_count} 個欄位值)")
        
        return True
        
    except Exception as e:
        platform = file_path.parent.name
        filename = file_path.name
        log_lines.append(f"資料清理失敗: {platform}/{filename} - {e}")
        return False

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

    # ============= 階段 1: 解壓縮和複製所有檔案到 temp 目錄 =============
    log_lines.append("=== 階段 1: 解壓縮和複製檔案 ===")
    
    # 處理每個平台的目錄
    for platform_dir in raw_dir.iterdir():
        if not platform_dir.is_dir():
            continue
            
        platform = platform_dir.name
        platform_temp_dir = temp_dir / platform
        ensure_dir(platform_temp_dir)
        
        # 批次解壓縮該平台目錄中的所有壓縮檔案
        extracted_dirs = batch_extract_archives(
            platform_dir, 
            platform_temp_dir, 
            platform, 
            data
        )
        
        if extracted_dirs:
            log_lines.append(f"平台 {platform} 解壓縮完成，共 {len(extracted_dirs)} 個檔案")
        
        # 複製非壓縮檔案
        for file_path in platform_dir.glob("*"):
            if not file_path.is_file():
                continue
                
            filename = file_path.name
            if filename.startswith("."):
                continue
                
            # 支援的檔案格式（排除已處理的壓縮檔案）
            if not filename.lower().endswith(('.xlsx', '.xls', '.csv')):
                continue
            
            # 直接複製檔案
            try:
                output_path = platform_temp_dir / filename
                shutil.copyfile(file_path, output_path)
                log_lines.append(f"複製檔案: {platform}/{filename}")
            except Exception as e:
                log_lines.append(f"複製失敗: {platform}/{filename} - {e}")

    # ============= 階段 2: 移除所有 Excel 檔案密碼 =============
    log_lines.append("\n=== 階段 2: 移除 Excel 檔案密碼 ===")
    
    for platform_dir in temp_dir.iterdir():
        if not platform_dir.is_dir():
            continue
            
        platform = platform_dir.name
        
        for file_path in platform_dir.glob("*"):
            if not file_path.is_file():
                continue
                
            filename = file_path.name
            if not filename.lower().endswith(('.xlsx', '.xls')):
                continue
                
            # 尋找匹配的帳號密碼
            matched_account = None
            
            # 首先嘗試根據平台名稱取得密碼
            platform_password = get_password_for_platform(platform, data)
            if platform_password:
                # 根據平台名稱找到對應的商店資訊
                for shop in data:
                    if shop.get('keywords'):
                        platform_keywords = {
                            'etmall': ['東森', '東森購物', '森森'],
                            'momo': ['MOMO購物中心', 'MOMO', '富邦', '富邦MOMO'],
                            'shopee': ['蝦皮', 'Shopee'],
                            'pchome': ['PC購物中心', 'PC', '網家'],
                            'yahoo': ['Yahoo', 'Yahoo購物中心', '雅虎購物中心', '雅虎']
                        }
                        keywords = platform_keywords.get(platform.lower(), [])
                        if any(kw in shop['keywords'] for kw in keywords):
                            matched_account = {
                                "name": shop['shop_name'],
                                "account": shop.get('shop_account', ''),
                                "password": platform_password
                            }
                            break
            
            # 如果沒有找到，使用原有的檔案名稱匹配邏輯
            if not matched_account:
                for shop_name, info in accounts.items():
                    if shop_name in filename or (info["account"] and info["account"] in filename):
                        matched_account = {"name": shop_name, **info}
                        break
            
            if matched_account:
                # 移除密碼
                try:
                    temp_file = platform_dir / f"temp_{filename}"
                    remove_password(str(file_path), str(temp_file), matched_account["password"])
                    
                    # 替換原檔案
                    file_path.unlink()
                    temp_file.rename(file_path)
                    
                    log_lines.append(f'{matched_account["name"]} 密碼移除成功: {platform}/{filename}')
                except Exception as e:
                    log_lines.append(f'{matched_account["name"]} 密碼移除失敗: {platform}/{filename} - {e}')
            else:
                log_lines.append(f"無匹配密碼: {platform}/{filename}")

    # ============= 階段 3: 轉換所有 Excel 檔案為 CSV =============
    log_lines.append("\n=== 階段 3: 轉換 Excel 為 CSV ===")
    
    for platform_dir in temp_dir.iterdir():
        if not platform_dir.is_dir():
            continue
            
        platform = platform_dir.name
        
        for file_path in platform_dir.glob("*"):
            if not file_path.is_file():
                continue
                
            filename = file_path.name
            
            if filename.lower().endswith(('.xlsx', '.xls')):
                # 轉換 Excel 為 CSV
                try:
                    csv_filename = filename.rsplit('.', 1)[0] + '.csv'
                    csv_path = platform_dir / csv_filename
                    
                    df = pd.read_excel(file_path)
                    df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                    
                    log_lines.append(f"轉換 CSV 成功: {platform}/{csv_filename}")
                    processed_files.append(f"{platform}/{csv_filename}")
                    
                except Exception as e:
                    log_lines.append(f"轉換 CSV 失敗: {platform}/{filename} - {e}")
                    # 轉換失敗時保留 Excel 檔案
                    processed_files.append(f"{platform}/{filename}")
                    continue
            elif filename.lower().endswith('.csv'):
                # 已經是 CSV，直接記錄
                log_lines.append(f"保留 CSV: {platform}/{filename}")
                processed_files.append(f"{platform}/{filename}")
            else:
                # 其他檔案類型
                processed_files.append(f"{platform}/{filename}")

    # ============= 階段 4: 清理 CSV 資料中的空格和換行符號 =============
    log_lines.append("\n=== 階段 4: 清理 CSV 資料 ===")
    
    cleaned_files_count = 0
    failed_cleanings = 0
    
    for platform_dir in temp_dir.iterdir():
        if not platform_dir.is_dir():
            continue
            
        for file_path in platform_dir.glob("*.csv"):
            if clean_csv_file(file_path, log_lines):
                cleaned_files_count += 1
            else:
                failed_cleanings += 1
    
    log_lines.append(f"資料清理統計: 成功 {cleaned_files_count} 個, 失敗 {failed_cleanings} 個")

    # ============= 階段 5: 刪除 Excel 檔案，只保留 CSV =============
    log_lines.append("\n=== 階段 5: 清理 Excel 檔案 ===")
    
    for platform_dir in temp_dir.iterdir():
        if not platform_dir.is_dir():
            continue
            
        platform = platform_dir.name
        
        # 刪除所有 temp_ 開頭的檔案
        for file_path in platform_dir.glob("temp_*"):
            try:
                file_path.unlink()
                log_lines.append(f"刪除臨時檔案: {platform}/{file_path.name}")
            except Exception as e:
                log_lines.append(f"刪除臨時檔案失敗: {platform}/{file_path.name} - {e}")
        
        # 刪除有對應 CSV 的 Excel 檔案
        for file_path in platform_dir.glob("*.xlsx"):
            try:
                csv_filename = file_path.stem + '.csv'
                csv_path = platform_dir / csv_filename
                
                if csv_path.exists():
                    # 對應的 CSV 檔案存在，刪除 Excel
                    file_path.unlink()
                    log_lines.append(f"刪除 Excel: {platform}/{file_path.name}")
                else:
                    log_lines.append(f"保留 Excel (無對應 CSV): {platform}/{file_path.name}")
            except Exception as e:
                log_lines.append(f"刪除 Excel 失敗: {platform}/{file_path.name} - {e}")
        
        for file_path in platform_dir.glob("*.xls"):
            try:
                csv_filename = file_path.stem + '.csv'
                csv_path = platform_dir / csv_filename
                
                if csv_path.exists():
                    # 對應的 CSV 檔案存在，刪除 Excel
                    file_path.unlink()
                    log_lines.append(f"刪除 Excel: {platform}/{file_path.name}")
                else:
                    log_lines.append(f"保留 Excel (無對應 CSV): {platform}/{file_path.name}")
            except Exception as e:
                log_lines.append(f"刪除 Excel 失敗: {platform}/{file_path.name} - {e}")

    # 輸出摘要
    log_lines.append(f"\n=== 處理摘要 ===")
    log_lines.append(f"總共處理 {len(processed_files)} 個檔案")
    log_lines.append(f"資料清理: 成功 {cleaned_files_count} 個 CSV 檔案")
    log_lines.append(f"處理的檔案: {', '.join(processed_files)}")
    log_lines.append(f"輸出目錄: {temp_dir}")

    # 寫入 log
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_lines))
    print(f"\n執行 log 已產生：{log_path}")
    print(f"處理摘要: 總共處理 {len(processed_files)} 個檔案")
    print(f"資料清理: 成功清理 {cleaned_files_count} 個 CSV 檔案")
    print(f"檔案已輸出到: {temp_dir}")

if __name__ == "__main__":
    main()
    if '--no-wait' not in sys.argv:
        try:
            input("\n✅ 執行完畢，請按 Enter 關閉視窗...")
        except:
            pass