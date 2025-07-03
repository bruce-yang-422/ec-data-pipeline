# scripts/excel_password_remover/main.py
# -*- coding: utf-8 -*-
"""
自動解壓縮＋多帳號 Excel 密碼移除＋完整 log
用途：
    - 支援多平台/多帳號/多密碼。
    - 將所有資料處理過程紀錄在 log。
    - 檢查密碼檔未處理檔案提示。
"""

from pathlib import Path
from remover import remove_password
from utils import ensure_dir, extract_zip_files, load_passwords_json
import datetime

def main():
    # 根目錄推算（從任何路徑啟動都可！）
    project_root = Path(__file__).resolve().parents[2]
    raw_dir = project_root / "data_raw" / "shopee"
    temp_dir = project_root / "temp" / "shopee"
    log_dir = project_root / "logs"
    passwords_json = project_root / "config" / "ec_shops_universal_passwords.json"

    ensure_dir(temp_dir)
    ensure_dir(log_dir)

    # 清空 log
    for file in log_dir.glob("*"):
        file.unlink()
    log_path = log_dir / f"execution_log_{datetime.datetime.now():%Y%m%d_%H%M%S}.txt"

    # 讀密碼
    data = load_passwords_json(passwords_json)
    # 以 shop_account 為 key
    accounts = {item.get("shop_account"): item.get("shop_name") for item in data}

    log_lines = []
    processed_accounts = set()

    # 掃描處理所有檔案
    for input_path in raw_dir.iterdir():
        if not input_path.is_file():
            continue
        filename = input_path.name
        # 跳過隱藏檔案
        if filename.startswith("."):
            continue

        # 自動處理壓縮檔
        if filename.lower().endswith('.zip'):
            try:
                extract_zip_files(str(input_path), str(temp_dir))
                log_lines.append(f"🗜️ 解壓縮成功: {filename}")
            except Exception as e:
                log_lines.append(f"❌ 解壓縮失敗 {filename}: {e}")
            continue

        # 處理 Excel 密碼移除
        matched_accounts = [account for account in accounts if account and account in filename]
        if not matched_accounts:
            log_lines.append(f"⚠️ 未在密碼表找到 account，檔案: {filename}")
            continue

        account = matched_accounts[0]
        name = accounts[account]
        password = None
        for item in data:
            if item.get("shop_account") == account:
                password = item.get("report_download_password")
                break
        if not password:
            log_lines.append(f"⚠️ 沒有找到 {account} 的密碼")
            continue

        output_path = temp_dir / filename
        try:
            remove_password(str(input_path), str(output_path), password)
            log_lines.append(f"✅ {name} ({account}) 處理成功，輸出至 {output_path}")
            processed_accounts.add(account)
        except Exception as e:
            log_lines.append(f"❌ {name} ({account}) 處理失敗: {e}")

    # 檢查哪些帳號沒被處理
    for account, name in accounts.items():
        if account and account not in processed_accounts:
            log_lines.append(f"⚠️ {name} ({account}) 在密碼表中，但未找到對應檔案")

    # 輸出 log
    with log_path.open("w", encoding="utf-8") as log_file:
        log_file.write("\n".join(log_lines))
    print(f"\n📄 執行 log 已產生：{log_path}")

if __name__ == "__main__":
    main()
    input("\n✅ 執行完畢，請按 Enter 關閉視窗...")
