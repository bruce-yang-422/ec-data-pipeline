import os
import hashlib
from pathlib import Path
import pandas as pd
from collections import defaultdict

def get_file_hash(file_path):
    """計算檔案的 MD5 雜湊值"""
    hash_md5 = hashlib.md5()
    try:
        with open(file_path, "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()
    except Exception as e:
        print(f"無法讀取檔案 {file_path}: {e}")
        return None

def get_excel_content_hash(file_path):
    """讀取 Excel 檔案內容並計算雜湊值"""
    try:
        if file_path.suffix.lower() == '.xlsx':
            df = pd.read_excel(file_path, engine='openpyxl')
        else:
            df = pd.read_excel(file_path, engine='xlrd')
        
        # 將 DataFrame 轉換為字串並計算雜湊
        content_str = df.to_string(index=False)
        return hashlib.md5(content_str.encode('utf-8')).hexdigest()
    except Exception as e:
        print(f"無法讀取 Excel 檔案 {file_path}: {e}")
        return None

def check_duplicates(directory):
    """檢查目錄下的重複檔案"""
    print(f"檢查目錄：{directory}")
    print("=" * 60)
    
    # 收集所有檔案
    files = []
    for file_path in Path(directory).glob("*"):
        if file_path.is_file():
            files.append(file_path)
    
    print(f"總共找到 {len(files)} 個檔案")
    print()
    
    # 按檔案大小分組
    size_groups = defaultdict(list)
    for file_path in files:
        size = file_path.stat().st_size
        size_groups[size].append(file_path)
    
    # 檢查相同大小的檔案
    duplicate_groups = []
    for size, file_list in size_groups.items():
        if len(file_list) > 1:
            duplicate_groups.append((size, file_list))
    
    print(f"發現 {len(duplicate_groups)} 組相同大小的檔案")
    print()
    
    # 檢查內容重複
    content_hashes = defaultdict(list)
    total_duplicates = 0
    
    for size, file_list in duplicate_groups:
        print(f"檔案大小: {size} bytes ({len(file_list)} 個檔案)")
        
        for file_path in file_list:
            # 先嘗試讀取 Excel 內容
            content_hash = get_excel_content_hash(file_path)
            if content_hash:
                content_hashes[content_hash].append(file_path)
            else:
                # 如果無法讀取 Excel，使用檔案雜湊
                file_hash = get_file_hash(file_path)
                if file_hash:
                    content_hashes[file_hash].append(file_path)
        
        # 顯示這組中的重複情況
        for content_hash, hash_files in content_hashes.items():
            if len(hash_files) > 1:
                print(f"  內容雜湊 {content_hash[:8]}... 重複 {len(hash_files)} 次:")
                for f in hash_files:
                    print(f"    - {f.name}")
                total_duplicates += len(hash_files) - 1
        
        print()
        content_hashes.clear()  # 清空，準備檢查下一組
    
    print(f"總共發現 {total_duplicates} 個內容重複的檔案")
    
    # 建議清理
    print("\n建議清理策略：")
    print("1. 保留原始檔案（沒有時間戳的）")
    print("2. 刪除帶時間戳的備份檔案")
    print("3. 檢查是否有其他內容不同的重複檔案")

if __name__ == "__main__":
    backup_dir = "data_raw/etmall/backup"
    if os.path.exists(backup_dir):
        check_duplicates(backup_dir)
    else:
        print(f"目錄 {backup_dir} 不存在")
