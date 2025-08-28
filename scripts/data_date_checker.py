"""
===============================================================================
資料日期完整性檢查工具 (Data Date Integrity Checker)
===============================================================================

📋 腳本用途：
    本腳本用於檢查電商訂單資料中的日期欄位完整性，確保資料品質和連續性。
    主要針對 order_date 欄位進行驗證，識別無效日期、缺失日期等資料品質問題。

🎯 核心重點：
    1. 多格式日期解析：支援多種日期格式的自動識別和解析
    2. 日期完整性檢查：檢測日期範圍內的缺失日期
    3. 資料品質報告：生成詳細的檢查報告，包含統計資訊和問題摘要
    4. 智能缺失日期格式化：將連續的缺失日期合併為區間顯示

🔧 主要功能：
    - 自動掃描 data_processed/merged 目錄下的所有 CSV 檔案
    - 解析和驗證 order_date 欄位的日期資料
    - 計算日期範圍和識別缺失日期
    - 生成詳細的檢查報告（TXT 格式）
    - 記錄操作日誌，便於追蹤和除錯

📊 輸出結果：
    - 控制台即時顯示檢查進度和結果摘要
    - 生成詳細的檢查報告檔案（temp/date_check_report_YYYYMMDD_HHMMSS.txt）
    - 記錄操作日誌（logs/data_date_checker.log）

🚀 使用場景：
    - 電商資料品質檢查
    - 訂單資料完整性驗證
    - 資料倉儲資料品質監控
    - 報表生成前的資料驗證

📁 輸入檔案：
    - 位置：data_processed/merged/*.csv
    - 要求：必須包含 order_date 欄位
    - 格式：支援多種日期格式（YYYY-MM-DD, YYYY/MM/DD, YYYYMMDD 等）

📈 檢查項目：
    - 日期欄位存在性檢查
    - 日期格式有效性驗證
    - 日期範圍計算
    - 缺失日期識別和統計
    - 資料品質指標計算

作者：EC Data Pipeline 團隊
版本：v1.0.0
更新日期：2025-08-19
===============================================================================
"""

import os
import pandas as pd
import glob
from datetime import datetime, timedelta
from pathlib import Path
import json

# 路徑設定
PROJECT_ROOT = Path(__file__).parent.parent
DATA_PROCESSED_DIR = PROJECT_ROOT / 'data_processed' / 'merged'
OUTPUT_DIR = PROJECT_ROOT / 'temp'
LOG_DIR = PROJECT_ROOT / 'logs'

# 確保目錄存在
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

def find_data_files():
    """尋找 data_processed/merged 下的所有 CSV 檔案"""
    pattern = DATA_PROCESSED_DIR / '*.csv'
    files = glob.glob(str(pattern))
    
    if not files:
        raise FileNotFoundError(f"找不到 CSV 檔案於 {DATA_PROCESSED_DIR}")
    
    print(f"📁 找到 {len(files)} 個 CSV 檔案：")
    for f in files:
        print(f"   - {Path(f).name}")
    
    return files

def parse_order_date(date_str):
    """解析 order_date 字串，支援多種格式"""
    if pd.isna(date_str) or str(date_str).strip() == '':
        return None
    
    date_str = str(date_str).strip()
    
    # 嘗試多種日期格式
    date_formats = [
        '%Y-%m-%d',
        '%Y/%m/%d',
        '%Y-%m-%d %H:%M:%S',
        '%Y/%m/%d %H:%M:%S',
        '%Y-%m-%d %H:%M',
        '%Y/%m/%d %H:%M',
        '%d/%m/%Y',
        '%m/%d/%Y',
        '%Y%m%d'
    ]
    
    for fmt in date_formats:
        try:
            parsed_date = pd.to_datetime(date_str, format=fmt)
            # 只返回日期部分，去除時間
            return parsed_date.normalize()
        except:
            continue
    
    # 如果所有格式都失敗，嘗試 pandas 自動解析
    try:
        parsed_date = pd.to_datetime(date_str)
        # 只返回日期部分，去除時間
        return parsed_date.normalize()
    except:
        return None

def check_file_dates(file_path):
    """檢查單一檔案的日期資料"""
    print(f"\n📖 檢查檔案：{Path(file_path).name}")
    
    try:
        # 讀取 CSV 檔案
        df = pd.read_csv(file_path, dtype=str)
        print(f"📊 總資料筆數：{len(df)}")
        
        # 檢查是否有 order_date 欄位
        if 'order_date' not in df.columns:
            print(f"❌ 檔案中沒有 order_date 欄位")
            return {
                'file_name': Path(file_path).name,
                'total_records': len(df),
                'has_order_date': False,
                'valid_dates': 0,
                'invalid_dates': 0,
                'date_range': None,
                'missing_dates': []
            }
        
        # 解析日期
        valid_dates = []
        invalid_count = 0
        
        for idx, row in df.iterrows():
            date_obj = parse_order_date(row['order_date'])
            if date_obj is not None:
                valid_dates.append(date_obj)
            else:
                invalid_count += 1
        
        print(f"✅ 有效日期：{len(valid_dates)} 筆")
        print(f"❌ 無效日期：{invalid_count} 筆")
        
        if not valid_dates:
            print(f"⚠️ 沒有有效的日期資料")
            return {
                'file_name': Path(file_path).name,
                'total_records': len(df),
                'has_order_date': True,
                'valid_dates': 0,
                'invalid_dates': invalid_count,
                'date_range': None,
                'missing_dates': []
            }
        
        # 計算日期範圍
        min_date = min(valid_dates)
        max_date = max(valid_dates)
        date_range = (min_date, max_date)
        
        print(f"📅 日期範圍：{min_date.strftime('%Y-%m-%d')} 到 {max_date.strftime('%Y-%m-%d')}")
        
        # 找出缺失的日期
        missing_dates = []
        current_date = min_date
        while current_date <= max_date:
            if current_date not in valid_dates:
                missing_dates.append(current_date.strftime('%Y-%m-%d'))
            current_date += timedelta(days=1)
        
        print(f"🔍 缺失日期：{len(missing_dates)} 天")
        if missing_dates:
            print(f"   前5個缺失日期：{missing_dates[:5]}")
        
        return {
            'file_name': Path(file_path).name,
            'total_records': len(df),
            'has_order_date': True,
            'valid_dates': len(valid_dates),
            'invalid_dates': invalid_count,
            'date_range': date_range,
            'missing_dates': missing_dates
        }
        
    except Exception as e:
        print(f"❌ 處理檔案時發生錯誤：{e}")
        return {
            'file_name': Path(file_path).name,
            'error': str(e)
        }

def generate_report(results):
    """生成報表"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = OUTPUT_DIR / f'date_check_report_{timestamp}.txt'
    
    def format_missing_dates(missing_dates):
        """將缺失日期格式化為區間顯示"""
        if not missing_dates:
            return []
        
        # 將字串日期轉換為 datetime 物件
        date_objects = [datetime.strptime(date, '%Y-%m-%d') for date in missing_dates]
        date_objects.sort()
        
        ranges = []
        start_date = end_date = date_objects[0]
        
        for i in range(1, len(date_objects)):
            current_date = date_objects[i]
            # 如果當前日期與前一個日期連續
            if (current_date - end_date).days == 1:
                end_date = current_date
            else:
                # 不連續，保存當前區間
                if start_date == end_date:
                    ranges.append(start_date.strftime('%Y-%m-%d'))
                else:
                    ranges.append(f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}")
                start_date = end_date = current_date
        
        # 處理最後一個區間
        if start_date == end_date:
            ranges.append(start_date.strftime('%Y-%m-%d'))
        else:
            ranges.append(f"{start_date.strftime('%Y-%m-%d')}~{end_date.strftime('%Y-%m-%d')}")
        
        return ranges
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write("=" * 80 + "\n")
        f.write("資料日期檢查報表\n")
        f.write("=" * 80 + "\n")
        f.write(f"生成時間：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"檢查檔案數：{len(results)}\n\n")
        
        # 總體統計
        total_files = len(results)
        files_with_order_date = sum(1 for r in results if r.get('has_order_date', False))
        total_records = sum(r.get('total_records', 0) for r in results if 'total_records' in r)
        total_valid_dates = sum(r.get('valid_dates', 0) for r in results if 'valid_dates' in r)
        total_invalid_dates = sum(r.get('invalid_dates', 0) for r in results if 'invalid_dates' in r)
        
        f.write("📊 總體統計\n")
        f.write("-" * 40 + "\n")
        f.write(f"檢查檔案數：{total_files}\n")
        f.write(f"包含 order_date 欄位的檔案：{files_with_order_date}\n")
        f.write(f"總資料筆數：{total_records:,}\n")
        f.write(f"有效日期筆數：{total_valid_dates:,}\n")
        f.write(f"無效日期筆數：{total_invalid_dates:,}\n")
        f.write(f"日期有效率：{total_valid_dates/(total_valid_dates+total_invalid_dates)*100:.1f}%\n\n")
        
        # 各檔案詳細資訊
        f.write("📁 各檔案詳細資訊\n")
        f.write("=" * 80 + "\n")
        
        for result in results:
            f.write(f"\n檔案名稱：{result['file_name']}\n")
            f.write("-" * 50 + "\n")
            
            if 'error' in result:
                f.write(f"❌ 錯誤：{result['error']}\n")
                continue
            
            f.write(f"總資料筆數：{result['total_records']:,}\n")
            
            if not result.get('has_order_date', False):
                f.write("❌ 沒有 order_date 欄位\n")
                continue
            
            f.write(f"有效日期：{result['valid_dates']:,} 筆\n")
            f.write(f"無效日期：{result['invalid_dates']:,} 筆\n")
            
            if result['date_range']:
                min_date, max_date = result['date_range']
                f.write(f"日期範圍：{min_date.strftime('%Y-%m-%d')} 到 {max_date.strftime('%Y-%m-%d')}\n")
                f.write(f"缺失日期數：{len(result['missing_dates'])} 天\n")
                
                if result['missing_dates']:
                    f.write("缺失日期列表：\n")
                    # 格式化為區間顯示
                    formatted_ranges = format_missing_dates(result['missing_dates'])
                    
                    if len(formatted_ranges) <= 20:
                        # 如果區間不多，全部顯示
                        for date_range in formatted_ranges:
                            f.write(f"  - {date_range}\n")
                    else:
                        # 如果區間很多，顯示前10個和後10個
                        f.write("  (顯示前10個和後10個)\n")
                        for date_range in formatted_ranges[:10]:
                            f.write(f"  - {date_range}\n")
                        f.write("  ...\n")
                        for date_range in formatted_ranges[-10:]:
                            f.write(f"  - {date_range}\n")
                        f.write(f"  (共 {len(formatted_ranges)} 個區間)\n")
                else:
                    f.write("✅ 沒有缺失日期\n")
        
        # 缺失日期摘要
        f.write("\n\n🔍 缺失日期摘要\n")
        f.write("=" * 80 + "\n")
        
        all_missing_dates = []
        for result in results:
            if 'missing_dates' in result and result['missing_dates']:
                all_missing_dates.extend(result['missing_dates'])
        
        if all_missing_dates:
            # 去重並排序
            unique_missing_dates = sorted(list(set(all_missing_dates)))
            f.write(f"總共有 {len(unique_missing_dates)} 個不同的缺失日期：\n")
            
            # 格式化為區間顯示
            formatted_ranges = format_missing_dates(unique_missing_dates)
            
            if len(formatted_ranges) <= 50:
                for date_range in formatted_ranges:
                    f.write(f"  - {date_range}\n")
            else:
                f.write("  (顯示前25個和後25個)\n")
                for date_range in formatted_ranges[:25]:
                    f.write(f"  - {date_range}\n")
                f.write("  ...\n")
                for date_range in formatted_ranges[-25:]:
                    f.write(f"  - {date_range}\n")
                f.write(f"  (共 {len(formatted_ranges)} 個區間)\n")
        else:
            f.write("✅ 所有檔案都沒有缺失日期\n")
    
    print(f"📄 報表已生成：{report_file}")
    return report_file

def main():
    """主要處理函數"""
    try:
        print("🚀 開始檢查資料日期...")
        
        # 尋找檔案
        data_files = find_data_files()
        
        # 檢查每個檔案
        results = []
        for file_path in data_files:
            result = check_file_dates(file_path)
            results.append(result)
        
        # 生成報表
        report_file = generate_report(results)
        
        print(f"\n🎉 檢查完成！")
        print(f"📁 檢查檔案數：{len(results)}")
        print(f"📄 報表位置：{report_file}")
        
        # 寫入 log
        log_file = LOG_DIR / 'data_date_checker.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 檢查完成\n")
            f.write(f"  檢查檔案數：{len(results)}, 報表：{report_file.name}\n")
        
        print(f"📝 詳細記錄已寫入：{log_file}")
        
    except Exception as e:
        print(f"❌ 錯誤：{e}")
        # 寫入錯誤 log
        log_file = LOG_DIR / 'data_date_checker.log'
        with open(log_file, 'a', encoding='utf-8') as f:
            f.write(f"{datetime.now().isoformat()} - 錯誤：{e}\n")

if __name__ == '__main__':
    main() 