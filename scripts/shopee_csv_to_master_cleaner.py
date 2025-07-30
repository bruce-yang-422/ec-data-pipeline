"""
shopee_csv_to_master_cleaner.py

功能：
- 批次讀取 temp/shopee/*.csv 檔案
- 按 shopee_fields_mapping.json 定義調整欄位
- 輸出到 shopee_master_orders_cleaned.csv

使用：python shopee_csv_to_master_cleaner.py

輸入：
- temp/shopee/*.csv 檔案
- config/shopee_fields_mapping.json

輸出：
- data_processed/merged/shopee_master_orders_cleaned.csv

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import pandas as pd
import json
from datetime import datetime
import os
from glob import glob
import re

MAPPING_PATH = "config/shopee_fields_mapping.json"
SOURCE_DIR = "temp/shopee"
OUTPUT_PATH = "data_processed/merged/shopee_master_orders_cleaned.csv"

def get_mapping():
    """讀取 shopee mapping 設定，按照 order 排序，並建立中英文對應"""
    try:
        with open(MAPPING_PATH, "r", encoding="utf-8") as f:
            mapping = json.load(f)
    except FileNotFoundError:
        print(f"警告: 找不到 mapping 檔案 {MAPPING_PATH}")
        return {}, [], {}, {}
    except json.JSONDecodeError as e:
        print(f"警告: mapping 檔案格式錯誤 {MAPPING_PATH}: {e}")
        return {}, [], {}, {}
    
    # 按照 order 欄位排序，確保欄位順序正確
    columns = sorted(mapping.keys(), key=lambda k: int(mapping[k].get("order", "999")))
    
    # 建立中英文對應（中文 -> 英文）
    zh2en = {v.get("zh_name", k): k for k, v in mapping.items() if v.get("zh_name")}
    
    # 建立英中文對應（英文 -> 中文）
    en2zh = {k: v.get("zh_name", k) for k, v in mapping.items()}
    
    print(f"載入 mapping: {len(columns)} 個欄位")
    print(f"中英對應: {len(zh2en)} 個對應關係")
    
    return mapping, columns, en2zh, zh2en

def parse_shop_info(filename, df):
    """解析店鋪資訊"""
    # 優先從檔案內容讀取
    if 'shop_name' in df.columns and 'shop_account' in df.columns:
        shop_name = df['shop_name'].dropna().iloc[0] if len(df['shop_name'].dropna()) > 0 else ""
        shop_account = df['shop_account'].dropna().iloc[0] if len(df['shop_account'].dropna()) > 0 else ""
        if shop_name and shop_account:
            return str(shop_name), str(shop_account)
    
    # 從檔名解析
    base = os.path.basename(filename)
    m = re.match(r"(.+?)_([\w\d]+)_Order", base)
    if m:
        return m.group(1), m.group(2)
    
    return "unknown_shop", "unknown_account"

def read_csv_files(zh2en_mapping):
    """讀取所有 CSV 檔案並使用 mapping 檔案進行欄位名稱對應"""
    csv_files = glob(os.path.join(SOURCE_DIR, "*.csv"))
    if not csv_files:
        return pd.DataFrame()
    
    dfs = []
    for file in csv_files:
        try:
            df = pd.read_csv(file, dtype=str, encoding='utf-8-sig').fillna("")
            
            print(f"\n處理檔案: {os.path.basename(file)}")
            print(f"原始欄位數: {len(df.columns)}")
            print(f"原始欄位範例: {list(df.columns)[:5]}...")
            
            # 使用 mapping 檔案進行欄位名稱對應
            original_columns = df.columns.tolist()
            mapped_columns = {}
            unmapped_columns = []
            
            for col in original_columns:
                # 清理欄位名稱：移除換行符號、多餘空白、標點符號差異
                col_cleaned = col.strip().replace('\n', '').replace('\r', '').replace('：', ':').replace('（', '(').replace('）', ')')
                col_cleaned = ' '.join(col_cleaned.split())  # 移除多餘空白
                
                found_match = False
                
                # 1. 直接對應（原欄位名）
                if col in zh2en_mapping:
                    mapped_columns[col] = zh2en_mapping[col]
                    found_match = True
                
                # 2. 清理後直接對應
                elif col_cleaned in zh2en_mapping:
                    mapped_columns[col] = zh2en_mapping[col_cleaned]
                    found_match = True
                
                # 3. 模糊匹配
                else:
                    for zh_name, en_name in zh2en_mapping.items():
                        zh_name_cleaned = zh_name.strip().replace('\n', '').replace('\r', '').replace('：', ':').replace('（', '(').replace('）', ')')
                        zh_name_cleaned = ' '.join(zh_name_cleaned.split())  # 移除多餘空白
                        
                        # 完全匹配（清理後）
                        if col_cleaned == zh_name_cleaned:
                            mapped_columns[col] = en_name
                            found_match = True
                            break
                        
                        # 包含關係匹配（避免過短的誤匹配）
                        elif len(col_cleaned) > 5 and len(zh_name_cleaned) > 5:
                            if col_cleaned in zh_name_cleaned or zh_name_cleaned in col_cleaned:
                                # 檢查相似度，避免誤匹配
                                shorter = min(len(col_cleaned), len(zh_name_cleaned))
                                longer = max(len(col_cleaned), len(zh_name_cleaned))
                                if shorter / longer > 0.7:  # 相似度超過70%
                                    mapped_columns[col] = en_name
                                    found_match = True
                                    break
                
                if not found_match:
                    unmapped_columns.append(col)
                    # 顯示清理後的欄位名，方便除錯
                    print(f"  無法對應: '{col}' -> 清理後: '{col_cleaned}'")
            
            print(f"成功對應: {len(mapped_columns)} 個欄位")
            print(f"無法對應: {len(unmapped_columns)} 個欄位")
            
            # 顯示成功對應的關鍵欄位
            key_mappings = {k: v for k, v in mapped_columns.items() if v in ['order_sn', 'product_name', 'product_sku_main', 'buyer_username']}
            if key_mappings:
                print(f"關鍵欄位對應: {key_mappings}")
            
            if unmapped_columns and len(unmapped_columns) <= 5:
                print(f"未對應欄位詳細: {unmapped_columns}")  # 少於5個就全部顯示
            
            # 應用欄位名稱對應
            df = df.rename(columns=mapped_columns)
            
            # 過濾空行（主要欄位都為空的行）
            if 'order_sn' in df.columns:
                before_filter = len(df)
                df = df[df['order_sn'].str.strip() != ""]
                print(f"過濾空訂單: {before_filter} -> {len(df)} 筆")
            elif '訂單編號' in df.columns:
                # 如果還是中文欄位名，也要處理
                before_filter = len(df)
                df = df[df['訂單編號'].str.strip() != ""]
                df = df.rename(columns={'訂單編號': 'order_sn'})
                print(f"過濾空訂單(中文欄位): {before_filter} -> {len(df)} 筆")
            
            # 清理換行符號和數字格式
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].astype(str).str.replace('\n', ' ').str.replace('\r', ' ').str.strip()
                    # 移除數字欄位的 .0 後綴
                    if col in ['product_sku_main', 'product_sku_variation', 'quantity', 'return_quantity']:
                        df[col] = df[col].str.replace(r'\.0$', '', regex=True)
            
            # 解析店鋪資訊
            shop_name, shop_account = parse_shop_info(file, df)
            df["shop_name"] = shop_name
            df["shop_account"] = shop_account
            
            # 處理 order_date - 從 order_creation_timestamp 或 order_sn 解析
            if 'order_creation_timestamp' in df.columns and 'order_date' not in df.columns:
                # 從完整時間戳解析日期
                df['order_date'] = pd.to_datetime(df['order_creation_timestamp'], errors='coerce').dt.strftime('%Y-%m-%d')
                df['order_date'] = df['order_date'].fillna('')
                print(f"從 order_creation_timestamp 解析 order_date")
            elif 'order_sn' in df.columns and 'order_date' not in df.columns:
                # 從 order_sn 前6碼解析日期 (YYMMDD -> YYYY-MM-DD)
                def parse_date_from_sn(sn):
                    try:
                        if len(str(sn)) >= 6:
                            date_part = str(sn)[:6]
                            year = int('20' + date_part[:2])
                            month = int(date_part[2:4])
                            day = int(date_part[4:6])
                            return f"{year:04d}-{month:02d}-{day:02d}"
                    except:
                        pass
                    return ''
                
                df['order_date'] = df['order_sn'].apply(parse_date_from_sn)
                print(f"從 order_sn 解析 order_date")
            
            print(f"最終欄位數: {len(df.columns)}")
            print(f"最終欄位範例: {[col for col in df.columns if col in ['order_sn', 'order_date', 'product_name', 'shop_name']]}")
            
            dfs.append(df)
            print(f"✅ 讀取成功: {len(df)} 筆資料")
            
        except Exception as e:
            print(f"❌ 讀取失敗: {file} - {e}")
    
    combined_df = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    print(f"\n📊 總計讀取: {len(combined_df)} 筆資料")
    return combined_df

def process_data(df, mapping, columns):
    """處理資料，完全參照 shopee_fields_mapping.json"""
    
    print(f"開始處理資料: {len(df)} 筆")
    
    # 基本欄位設定（參照 mapping 檔案順序）
    df['platform'] = 'shopee'  # order: 1, 固定值
    df['processing_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # order: 4
    
    # 產生 item_seq (order: 7)
    if 'order_date' in df.columns and 'order_sn' in df.columns:
        df = df.sort_values(['order_date', 'order_sn']).reset_index(drop=True)
        df['item_seq'] = df.groupby(['order_date', 'order_sn']).cumcount() + 1
        print(f"產生 item_seq 完成")
    else:
        df['item_seq'] = 1
        print(f"使用預設 item_seq = 1")
    
    # 檢查關鍵欄位
    print(f"order_date 欄位存在: {'order_date' in df.columns}")
    print(f"order_sn 欄位存在: {'order_sn' in df.columns}")
    print(f"product_sku_main 欄位存在: {'product_sku_main' in df.columns}")
    
    if 'order_date' in df.columns:
        empty_order_date = len(df[df['order_date'].fillna('').astype(str).str.strip() == ''])
        print(f"空的 order_date: {empty_order_date} 筆")
    
    if 'order_sn' in df.columns:
        empty_order_sn = len(df[df['order_sn'].fillna('').astype(str).str.strip() == ''])
        print(f"空的 order_sn: {empty_order_sn} 筆")
    
    # 建立 key_for_merge (order: 60) - 按照 mapping 定義
    # order_sn + product_sku_main + product_sku_variation
    order_sn_part = df['order_sn'].fillna('').astype(str) if 'order_sn' in df.columns else pd.Series([''] * len(df))
    sku_main_part = df['product_sku_main'].fillna('').astype(str) if 'product_sku_main' in df.columns else pd.Series([''] * len(df))
    sku_variation_part = df['product_sku_variation'].fillna('').astype(str) if 'product_sku_variation' in df.columns else pd.Series([''] * len(df))
    
    df['key_for_merge'] = order_sn_part + '_' + sku_main_part + '_' + sku_variation_part
    df['duplicate_key'] = df['key_for_merge']  # 用於去重
    
    print(f"建立 key_for_merge 完成，範例: {df['key_for_merge'].iloc[0] if len(df) > 0 else 'N/A'}")
    
    # 修正：一次性添加缺失的欄位，避免碎片化
    missing_cols = [col for col in columns if col not in df.columns]
    print(f"缺失欄位: {len(missing_cols)} 個")
    
    if missing_cols:
        # 根據 mapping 設定預設值
        missing_data = {}
        for col in missing_cols:
            if col in mapping:
                col_type = mapping[col].get('type', 'STRING')
                if col_type in ['NUMERIC', 'INTEGER']:
                    missing_data[col] = 0
                elif col_type in ['DATE', 'TIMESTAMP']:
                    missing_data[col] = ''
                else:  # STRING
                    missing_data[col] = ''
            else:
                missing_data[col] = ''
        
        # 建立缺失欄位的 DataFrame
        missing_df = pd.DataFrame(missing_data, index=df.index)
        # 一次性合併所有缺失欄位
        df = pd.concat([df, missing_df], axis=1)
        print(f"添加缺失欄位完成")
    
    # 根據 mapping 檔案設定資料類型
    for col in columns:
        if col in mapping:
            col_type = mapping[col].get('type', 'STRING')
            
            # 數值類型處理
            if col_type == 'NUMERIC':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)
            elif col_type == 'INTEGER':
                df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype('Int64')  # 支援 null 的整數
            
            # 日期時間處理
            elif col_type in ['DATE', 'TIMESTAMP']:
                # 保持字串格式，讓後續處理決定日期格式
                df[col] = df[col].astype(str).replace('nan', '').replace('0', '').replace('0.0', '')
            
            # 字串類型處理
            else:  # STRING
                df[col] = df[col].astype(str).replace('nan', '').replace('None', '')
    
    print(f"資料類型轉換完成")
    
    # 按照 mapping 檔案的 order 順序重新排列欄位
    ordered_columns = sorted([col for col in columns if col in df.columns], 
                           key=lambda x: int(mapping.get(x, {}).get('order', '999')))
    
    # 確保關鍵欄位包含在結果中
    result_columns = ordered_columns + ['duplicate_key']
    
    # 添加額外的系統欄位（不在 mapping 中但需要的）
    extra_columns = ['key_for_merge']  # 移除 data_import_timestamp，這個在 save_data 中處理
    for col in extra_columns:
        if col in df.columns and col not in result_columns:
            result_columns.append(col)
    
    print(f"最終欄位數: {len(result_columns)} 個")
    
    return df[result_columns]

def save_data(df, mapping):
    """儲存資料，智慧新資料覆蓋舊資料"""
    os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)
    
    print(f"準備儲存資料: {len(df)} 筆")
    
    # 為新資料添加匯入時間戳
    current_timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df['data_import_timestamp'] = current_timestamp
    
    # 合併現有資料
    if os.path.exists(OUTPUT_PATH):
        try:
            old_df = pd.read_csv(OUTPUT_PATH, dtype=str).fillna("")
            print(f"讀取到舊資料: {len(old_df)} 筆")
            
            # 確保舊資料也有匯入時間戳
            if 'data_import_timestamp' not in old_df.columns:
                old_df['data_import_timestamp'] = '2024-01-01 00:00:00'  # 預設舊時間
            
            # 重置索引避免衝突
            old_df = old_df.reset_index(drop=True)
            df = df.reset_index(drop=True)
            
            # 合併資料
            combined = pd.concat([old_df, df], ignore_index=True)
            print(f"合併後總計: {len(combined)} 筆")
            
            # 智慧去重：根據 duplicate_key 去重，保留最新的資料
            if 'duplicate_key' in combined.columns:
                before_dedup = len(combined)
                
                print(f"去重前詳細分析:")
                
                # 檢查 duplicate_key 的分布
                dup_counts = combined['duplicate_key'].value_counts()
                unique_keys = len(dup_counts)
                total_duplicates = len(dup_counts[dup_counts > 1])
                
                print(f"  - 唯一的 duplicate_key: {unique_keys} 個")
                print(f"  - 有重複的 key: {total_duplicates} 個")
                print(f"  - 最大重複次數: {dup_counts.max()}")
                
                # 檢查時間戳分布
                new_data_count = len(combined[combined['data_import_timestamp'] == current_timestamp])
                old_data_count = before_dedup - new_data_count
                print(f"  - 新資料標記: {new_data_count} 筆")
                print(f"  - 舊資料標記: {old_data_count} 筆")
                
                # 轉換時間戳為 datetime 進行比較
                combined['temp_timestamp'] = pd.to_datetime(combined['data_import_timestamp'], errors='coerce')
                
                # 檢查時間戳轉換結果
                valid_timestamps = combined['temp_timestamp'].notna().sum()
                print(f"  - 有效時間戳: {valid_timestamps} 筆")
                
                # 顯示時間戳範例
                timestamp_sample = combined[['duplicate_key', 'data_import_timestamp', 'temp_timestamp']].head(3)
                print(f"  - 時間戳範例:")
                for _, row in timestamp_sample.iterrows():
                    print(f"    Key: {row['duplicate_key'][:20]}..., Import: {row['data_import_timestamp']}, Parsed: {row['temp_timestamp']}")
                
                # 分析重複資料
                if total_duplicates > 0:
                    print(f"\n重複 key 分析（前5個）:")
                    for key, count in dup_counts.head(5).items():
                        if count > 1:
                            subset = combined[combined['duplicate_key'] == key][['data_import_timestamp', 'temp_timestamp', 'order_sn']]
                            print(f"  Key: {key[:30]}... (重複 {count} 次)")
                            for _, row in subset.iterrows():
                                print(f"    - {row['order_sn']}: {row['data_import_timestamp']}")
                
                # 執行去重：按 duplicate_key 分組，保留時間戳最新的記錄
                print(f"\n執行去重...")
                
                # 方法修正：使用 sort_values + drop_duplicates 更安全
                combined_sorted = combined.sort_values(['duplicate_key', 'temp_timestamp'], na_position='first')
                combined_deduped = combined_sorted.drop_duplicates(subset=['duplicate_key'], keep='last')
                combined = combined_deduped.drop(columns=['temp_timestamp']).reset_index(drop=True)
                
                after_dedup = len(combined)
                removed_count = before_dedup - after_dedup
                
                print(f"去重結果: {before_dedup} -> {after_dedup} 筆 (移除 {removed_count} 筆重複)")
                
                # 統計最終結果
                final_new_count = len(combined[combined['data_import_timestamp'] == current_timestamp])
                final_old_count = after_dedup - final_new_count
                
                print(f"最終資料組成:")
                print(f"  - 新資料: {final_new_count} 筆")
                print(f"  - 保留舊資料: {final_old_count} 筆")
                
                # 驗證去重邏輯
                if removed_count > old_data_count:
                    print(f"⚠️  警告: 移除數量({removed_count}) > 舊資料數量({old_data_count})，可能有問題")
                
                if final_old_count == 0 and old_data_count > 0:
                    print(f"⚠️  警告: 所有舊資料都被移除，這可能不正常")
                    
                    # 分析原因
                    print(f"分析原因:")
                    if total_duplicates >= unique_keys * 0.8:
                        print(f"  - 可能原因: duplicate_key 重複率過高 ({total_duplicates}/{unique_keys})")
                    if valid_timestamps < before_dedup * 0.9:
                        print(f"  - 可能原因: 時間戳轉換失敗率過高")
                
            else:
                print("⚠️  警告: 找不到 duplicate_key 欄位，跳過去重")
                
        except Exception as e:
            print(f"讀取舊檔案失敗，將建立新檔案: {e}")
            df['data_import_timestamp'] = current_timestamp
            combined = df
    else:
        df['data_import_timestamp'] = current_timestamp
        combined = df
        print("建立新檔案")
    
    # 重新排序：order_date 升序，然後按訂單編號排序
    if 'order_date' in combined.columns and 'order_sn' in combined.columns:
        # 處理空值
        combined['order_date'] = combined['order_date'].fillna('')
        combined['order_sn'] = combined['order_sn'].fillna('')
        
        print(f"排序前: {len(combined)} 筆")
        
        # 先按日期，再按訂單編號排序
        combined = combined.sort_values(['order_date', 'order_sn', 'item_seq']).reset_index(drop=True)
        
        # 重新計算 item_seq（確保同一訂單內的商品序號正確）
        combined['item_seq'] = combined.groupby(['order_date', 'order_sn']).cumcount() + 1
        
        # 檢查空訂單
        empty_orders = combined[(combined['order_date'] == '') & (combined['order_sn'] == '')]
        if len(empty_orders) > 0:
            print(f"⚠️  發現 {len(empty_orders)} 筆空訂單資料，建議檢查來源檔案")
            # 可選：將空訂單移至檔案末尾
            valid_orders = combined[~((combined['order_date'] == '') & (combined['order_sn'] == ''))]
            combined = pd.concat([valid_orders, empty_orders], ignore_index=True)
        
        print(f"排序後: {len(combined)} 筆")
    
    # 移除臨時欄位
    if 'duplicate_key' in combined.columns:
        combined = combined.drop(columns=['duplicate_key'])
    
    # 確保欄位順序正確（按照 mapping 的 order）
    if mapping:
        # 標準欄位按順序排列
        standard_columns = [col for col in combined.columns if col in mapping]
        ordered_columns = sorted(standard_columns, key=lambda x: int(mapping.get(x, {}).get('order', '999')))
        
        # 額外欄位添加到最後
        extra_columns = [col for col in combined.columns if col not in mapping]
        final_columns = ordered_columns + extra_columns
        
        combined = combined[final_columns]
    
    # 儲存檔案
    combined.to_csv(OUTPUT_PATH, index=False, encoding='utf-8-sig')
    print(f"✅ 已儲存: {OUTPUT_PATH} ({len(combined)} 筆)")
    
    # 顯示資料摘要
    if 'order_date' in combined.columns:
        date_range = combined[combined['order_date'] != '']['order_date'].agg(['min', 'max'])
        if not date_range.empty and date_range['min'] == date_range['min']:  # 檢查非 NaN
            print(f"📅 資料日期範圍: {date_range['min']} ~ {date_range['max']}")
    
    if 'data_import_timestamp' in combined.columns:
        latest_import = combined['data_import_timestamp'].max()
        print(f"🕒 最新匯入時間: {latest_import}")
    
    print(f"📋 最終欄位順序: {list(combined.columns)}")
    return len(combined)

def main():
    print("Shopee CSV 轉換器")
    
    # 讀取設定（包含中英文對應）
    mapping, columns, en2zh, zh2en = get_mapping()
    if not mapping:
        print("無法載入 mapping 設定，程式結束")
        return
    
    print(f"📋 已載入 {len(zh2en)} 個中英文欄位對應")
    
    # 讀取檔案（使用動態中英文對應）
    df = read_csv_files(zh2en)
    if df.empty:
        print("沒有找到 CSV 檔案")
        return
    
    print(f"\n📊 讀取 {len(df)} 筆資料")
    
    # 處理資料
    processed_df = process_data(df, mapping, columns)
    print(f"✅ 處理完成 {len(processed_df)} 筆")
    
    # 儲存
    final_count = save_data(processed_df, mapping)
    
    # 清理 temp 檔案
    try:
        for f in os.listdir(SOURCE_DIR):
            if f.lower().endswith('.csv'):
                os.unlink(os.path.join(SOURCE_DIR, f))
        print("🧹 已清理臨時檔案")
    except Exception as e:
        print(f"⚠️  清理臨時檔案時發生錯誤: {e}")
    
    print(f"🎉 完成！最終資料: {final_count} 筆")

if __name__ == "__main__":
    main()