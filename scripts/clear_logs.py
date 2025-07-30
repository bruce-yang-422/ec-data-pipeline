# scripts/clear_logs.py
"""
清除日誌檔案工具

功能：
- 清除 logs 資料夾下所有檔案
- 用於清理舊的日誌檔案，釋放磁碟空間

輸入：logs 資料夾路徑
輸出：清除所有日誌檔案，顯示清除數量

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import os

def clear_logs(logs_dir="logs"):
    """
    清除 logs 資料夾下所有檔案
    """
    full_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), logs_dir)

    if not os.path.exists(full_path):
        print(f"❌ 找不到 logs 資料夾: {full_path}")
        return

    files = os.listdir(full_path)
    removed_count = 0

    for file in files:
        file_path = os.path.join(full_path, file)
        if os.path.isfile(file_path):
            os.remove(file_path)
            removed_count += 1

    print(f"✅ 已刪除 {removed_count} 個檔案於 {logs_dir}/")


if __name__ == "__main__":
    clear_logs()
