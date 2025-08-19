"""
ETMall 大平台貼單清洗腳本

功能：
- 清洗 data_raw/etmall/大平台貼單 - 東森*.csv 檔案
- 標準化欄位名稱和資料格式
- 移除無效資料行和重複資料
- 處理特殊字元和非標準格式
- 輸出清洗後的中間檔供後續處理

輸入：data_raw/etmall/大平台貼單 - 東森*.csv
輸出：temp/etmall/02_01_etmall_platform_orders_cleaned_*.csv
"""

import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional, Dict, Any
import pandas as pd
import re


def setup_logging() -> None:
    """設定日誌"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def find_platform_order_files(data_raw_dir: Path) -> List[Path]:
    """尋找大平台貼單檔案"""
    pattern = "大平台貼單 - 東森*.csv"
    files = list(data_raw_dir.glob(pattern))
    if not files:
        logging.warning(f'在 {data_raw_dir} 中找不到 {pattern} 檔案')
        return []
    
    # 按修改時間排序，最新的在前
    files.sort(key=lambda x: x.stat().st_mtime, reverse=True)
    logging.info(f'找到 {len(files)} 個大平台貼單檔案')
    for f in files:
        logging.info(f'  - {f.name}')
    
    return files


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """清理欄位名稱"""
    logging.info('開始清理欄位名稱')
    
    # 移除空白欄位（但保留重要欄位）
    # 定義重要欄位，即使都是 NaN 也要保留
    important_columns = ['成本', '利潤', '對帳金額', '對帳用品編號']
    
    # 移除空白欄位，但保留重要欄位
    df = df.dropna(how='all', axis=1)
    
    # 確保重要欄位存在，如果被移除了就重新加入
    for col in important_columns:
        if col not in df.columns:
            # 對於成本欄位，保持空值；其他欄位填充 pd.NA
            if col == '成本':
                df[col] = pd.Series([''] * len(df), dtype='string')
            else:
                df[col] = pd.NA
    
    # 標準化欄位名稱 - 根據實際資料結構更新
    column_mapping = {
        'Unnamed: 0': 'shipping_carrier',  # 第1欄：物流公司
        '訂單編號': 'order_sn',
        '出貨商品編號': 'product_sale_id',
        '顏色': 'color',
        '款式': 'note_2',
        '商品名稱': 'product_name_platform',
        '數量': 'quantity',
        '單價': 'unit_price',
        '提貨人姓名': 'customer_name',
        '提貨人郵遞區號': 'shipping_zipcode',
        '提貨人地址': 'shipping_address',
        '提貨人日間電話': 'customer_tel_day',
        '提貨人夜間電話': 'customer_tel_night',
        '提貨人行動電話': 'customer_phone',
        '出貨客戶(平台)名稱': 'platform',
        '出貨客戶(平台)網址': 'platform_url',
        '出貨客戶(平台)客服電話': 'platform_service_tel',
        '備註': 'note_1',
        # '付款方式': 移除，不需要
        '代收總金額': 'cod_amount',
        '送達日': 'shipping_expected_date',
        '1:早,2:午,3:晚': 'shipping_expected_time',
        '件數': 'package_count',
        '末五碼': 'bank_account_last5',
        # '付款方式.1': 移除，重複欄位
        '發票編號': 'invoice_no',
        '訂單金額': 'order_amount',
        '對帳金額': 'reconciliation_amount',
        '成本': 'cost_to_platform',
        '利潤': 'profit',
        '額外運費': 'extra_shipping_fee',
        '對帳用品編號': 'reconciliation_item_no',
        # 'Unnamed: 32': 移除，空欄位
        '訂單日期': 'order_date'
    }
    
    # 要排除的欄位
    excluded_columns = ['付款方式', '付款方式.1', 'Unnamed: 32', '對帳銷貨編號', '出貨客戶(平台)網址', '出貨客戶(平台)客服電話']
    
    # 先移除不需要的欄位
    columns_to_keep = [col for col in df.columns if col not in excluded_columns]
    df = df[columns_to_keep]
    
    # 重新命名欄位
    new_columns = []
    for col in df.columns:
        if col in column_mapping:
            new_columns.append(column_mapping[col])
        else:
            # 處理未知欄位，移除特殊字元
            clean_col = re.sub(r'[^\w\s]', '', str(col)).strip()
            if clean_col:
                new_columns.append(clean_col)
            else:
                new_columns.append(f'unknown_col_{len(new_columns)}')
    
    df.columns = new_columns
    logging.info(f'欄位名稱清理完成，共 {len(df.columns)} 個欄位')
    
    # 欄位名稱清理完成後，立即強制將需要字串格式的欄位轉換為字串類型
    string_required_columns = ['product_sale_id', 'color', 'invoice_no', 'reconciliation_item_no', 'customer_phone', 'customer_tel_day', 'customer_tel_night']
    for col in string_required_columns:
        if col in df.columns:
            # 強制轉換為字串類型，避免被 pandas 自動轉換為數值
            df[col] = df[col].astype(str)
            logging.info(f'欄位名稱清理後，強制將 {col} 轉換為字串類型')
    
    return df


def clean_data_content(df: pd.DataFrame) -> pd.DataFrame:
    """清理資料內容"""
    logging.info('開始清理資料內容')
    
    # 移除完全空白的行
    df = df.dropna(how='all')
    
    # 特別處理電話欄位，在開始就強制轉換為字串類型
    phone_columns = ['customer_phone', 'customer_tel_day', 'customer_tel_night']
    for col in phone_columns:
        if col in df.columns:
            # 強制轉換為字串類型，避免被 pandas 自動轉換為數值
            df[col] = df[col].astype(str)
            logging.info(f'電話欄位 {col} 強制轉換為字串類型')
    
    # 特別處理其他需要字串格式的欄位
    string_required_columns = ['product_sale_id', 'color', 'invoice_no', 'reconciliation_item_no']
    for col in string_required_columns:
        if col in df.columns:
            # 強制轉換為字串類型，避免被 pandas 自動轉換為數值
            df[col] = df[col].astype(str)
            logging.info(f'字串欄位 {col} 強制轉換為字串類型')
    
                # 特別處理價格欄位，確保有小數點格式
            price_columns = ['unit_price', 'order_amount', 'reconciliation_amount', 'cod_amount']
            for col in price_columns:
                if col in df.columns:
                    # 處理價格格式：確保有小數點，空值預設為 0.00
                    def format_price(price_value):
                        if pd.isna(price_value) or str(price_value).strip() in ['', 'nan', 'None', 'NULL', 'null', 'NaN', 'NAN']:
                            return '0.00'
                        try:
                            # 轉換為浮點數
                            price_float = float(price_value)
                            # 格式化為兩位小數
                            return f'{price_float:.2f}'
                        except (ValueError, TypeError):
                            return '0.00'
                    
                    df[col] = df[col].apply(format_price)
                    logging.info(f'價格欄位 {col} 格式化完成，確保小數點格式，空值預設為 0.00')
    
    # 處理特殊值，轉換為空白字串而不是 None
    df = df.replace({
        '#N/A': '',
        'nan': '',
        'None': '',
        '': ''
    })
    
    # 清理字串欄位
    string_columns = df.select_dtypes(include=['object']).columns
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            # 移除換行符號
            df[col] = df[col].str.replace('\n', ' ').str.replace('\r', ' ')
            # 將各種空值轉換為空白字串
            df[col] = df[col].replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN'], '')
            df[col] = df[col].fillna('')
    
    # 處理數值欄位的空值
    numeric_columns = df.select_dtypes(include=['number']).columns
    for col in numeric_columns:
        if col in df.columns:
            # 將數值欄位的 NaN 轉換為空白字串
            df[col] = df[col].fillna('')
            # 將 'nan' 字串也轉換為空白字串
            df[col] = df[col].replace('nan', '')
    
    # 處理數量欄位
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).astype(int)
    
    # 處理價格欄位
    price_columns = ['unit_price', 'total_amount', 'order_amount', 'reconciliation_amount', 'profit', 'extra_shipping_fee']
    for col in price_columns:
        if col in df.columns:
            # 移除 NT$、$、逗號等字元
            df[col] = df[col].astype(str).str.replace('NT$', '').str.replace('$', '').str.replace(',', '')
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)
    
    # 處理日期欄位
    if 'order_date' in df.columns:
        df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
    
    if 'delivery_date' in df.columns:
        df['delivery_date'] = pd.to_datetime(df['delivery_date'], errors='coerce')
    
    # 處理郵遞區號
    if 'postal_code' in df.columns:
        df['postal_code'] = df['postal_code'].astype(str).str.extract(r'(\d+)')[0]
    
    # 標準化平台名稱
    if 'platform_name' in df.columns:
        df['platform_name'] = df['platform_name'].fillna('東森購物')
    
    # 特別處理 cost_to_platform 欄位，確保空值保持為空字串
    if 'cost_to_platform' in df.columns:
        # 強制轉換為字串類型，然後處理空值
        df['cost_to_platform'] = df['cost_to_platform'].astype(str)
        df['cost_to_platform'] = df['cost_to_platform'].fillna('')
        # 將 'nan' 字串也轉換為空字串
        df['cost_to_platform'] = df['cost_to_platform'].replace('nan', '')
        df['cost_to_platform'] = df['cost_to_platform'].replace('None', '')
    
    # 特別處理 note_1 欄位，移除預設內容 "配送前請先電聯，謝謝！"
    if 'note_1' in df.columns:
        df['note_1'] = df['note_1'].astype(str)
        # 移除預設內容
        df['note_1'] = df['note_1'].replace('配送前請先電聯，謝謝！', '')
        df['note_1'] = df['note_1'].replace('nan', '')
        df['note_1'] = df['note_1'].fillna('')
        logging.info('note_1 欄位處理完成，移除預設內容 "配送前請先電聯，謝謝！"')
    
    # 特別處理 note_2 欄位（原款式欄位）
    if 'note_2' in df.columns:
        df['note_2'] = df['note_2'].astype(str)
        df['note_2'] = df['note_2'].replace('nan', '')
        df['note_2'] = df['note_2'].fillna('')
        logging.info('note_2 欄位處理完成')
    
    # 特別處理電話欄位，確保以字串方式處理，不要有小數點或科學記號
    phone_columns = ['customer_phone', 'customer_tel_day', 'customer_tel_night']
    for col in phone_columns:
        if col in df.columns:
            # 強制轉換為字串類型
            df[col] = df[col].astype(str)
            # 移除小數點和科學記號，只保留數字和連字號
            df[col] = df[col].str.replace(r'\.0+$', '', regex=True)  # 移除結尾的 .0
            df[col] = df[col].str.replace(r'e[+-]\d+', '', regex=True, flags=re.IGNORECASE)  # 移除科學記號
            df[col] = df[col].str.replace(r'\.\d+', '', regex=True)  # 移除小數點後的所有數字
            
            # 特別處理手機號碼格式：如果是9碼且開頭是9，在開頭增加"0"
            def fix_phone_format(phone_str):
                if pd.isna(phone_str) or phone_str in ['', 'nan', 'None', 'NULL', 'null', 'NaN', 'NAN']:
                    return ''
                phone_str = str(phone_str).strip()
                # 如果是9碼且開頭是9，視為手機號碼，在開頭增加"0"
                if len(phone_str) == 9 and phone_str.startswith('9') and phone_str.isdigit():
                    return '0' + phone_str
                return phone_str
            
            df[col] = df[col].apply(fix_phone_format)
            # 處理空值
            df[col] = df[col].replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN'], '')
            df[col] = df[col].fillna('')
            logging.info(f'電話欄位 {col} 處理完成，確保字串格式並修正手機號碼格式')
    
    logging.info(f'資料內容清理完成，剩餘 {len(df)} 筆資料')
    
    return df


def remove_invalid_rows(df: pd.DataFrame) -> pd.DataFrame:
    """移除無效資料行"""
    logging.info('開始移除無效資料行')
    
    initial_count = len(df)
    
    # 移除沒有訂單編號的行，同時排除訂單編號尾巴含有 -1、-2、-3 的資料
    # 同時適用中英文欄位名稱
    order_sn_columns = ['order_sn', '訂單編號']
    order_sn_col = None
    
    # 尋找訂單編號欄位（優先使用英文）
    for col in order_sn_columns:
        if col in df.columns:
            order_sn_col = col
            break
    
    if order_sn_col:
        # 排除訂單編號尾巴含有 -1、-2、-3 的資料，同時跳過沒有值的資料
        # 檢查是否為 NaN、空字串、'nan' 等無效值
        df = df[
            (df[order_sn_col].notna()) &  # 排除 pandas NaN
            (df[order_sn_col].astype(str).str.strip() != '') &  # 排除空字串
            (df[order_sn_col].astype(str).str.strip() != 'nan') &  # 排除字串 'nan'
            (df[order_sn_col].astype(str).str.strip() != 'None') &  # 排除字串 'None'
            (~df[order_sn_col].astype(str).str.strip().str.endswith(('-1', '-2', '-3')))  # 排除尾碼
        ]
        logging.info(f'使用欄位 "{order_sn_col}" 進行訂單編號過濾，跳過空值資料')
    else:
        logging.warning('找不到訂單編號欄位，跳過訂單編號過濾')
    
    # 跳過商品編號篩選，保留所有資料行
    # 資料保留判斷依據：以訂單編號為主，只要訂單編號有效就保留
    logging.info('跳過商品編號篩選，保留所有資料行（包括沒有商品編號的運費、手續費等項目）')
    
    # 保留所有數量為 0 的行，資料保留判斷依據以訂單編號為主
    # 同時適用中英文欄位名稱
    quantity_columns = ['quantity', '數量']
    product_name_columns = ['product_name_platform', '商品名稱']
    
    quantity_col = None
    product_name_col = None
    
    # 尋找數量欄位（優先使用英文）
    for col in quantity_columns:
        if col in df.columns:
            quantity_col = col
            break
    
    # 尋找商品名稱欄位（優先使用英文）
    for col in product_name_columns:
        if col in df.columns:
            product_name_col = col
            break
    
    # 不進行數量篩選，保留所有資料行
    # 資料保留判斷依據：以訂單編號為主，只要訂單編號有效就保留
    logging.info('跳過數量篩選，保留所有資料行（包括數量為 0 的運費、手續費等項目）')
    
    # 跳過測試資料篩選，保留所有資料行
    # 運費項目（偏遠運費、補運費、一般運費）都是合法的費用項目，應該保留
    logging.info('跳過測試資料篩選，保留所有資料行（包括運費項目）')
    
    final_count = len(df)
    removed_count = initial_count - final_count
    
    logging.info(f'移除無效資料行完成，移除 {removed_count} 行，剩餘 {final_count} 行')
    
    return df


def add_derived_fields(df: pd.DataFrame) -> pd.DataFrame:
    """新增衍生欄位"""
    logging.info('開始新增衍生欄位')
    
    # 新增平台標識
    df['platform'] = 'etmall'
    
    # 新增 item_no 欄位：以同一個訂單編號為群組，依序給流水號 01、02、03...
    if 'order_sn' in df.columns:
        # 按訂單編號分組，然後給每個組內的行分配流水號
        df['item_no'] = df.groupby('order_sn').cumcount() + 1
        # 格式化為兩位數的流水號（01、02、03...）
        df['item_no'] = df['item_no'].apply(lambda x: f'{x:02d}')
        logging.info(f'新增 item_no 欄位完成，流水號範圍：01-{df["item_no"].max()}')
    
    # 新增訂單行唯一識別碼：order_sn + item_no
    if 'order_sn' in df.columns and 'item_no' in df.columns:
        df['order_line_uid'] = df['order_sn'].astype(str) + '_' + df['item_no'].astype(str)
        logging.info('新增 order_line_uid 欄位完成')
    
    # 處理 note_1 欄位，移除預設內容 "配送前請先電聯，謝謝！"
    if 'note_1' in df.columns:
        df['note_1'] = df['note_1'].astype(str)
        # 移除預設內容
        df['note_1'] = df['note_1'].replace('配送前請先電聯，謝謝！', '')
        df['note_1'] = df['note_1'].replace('nan', '')
        df['note_1'] = df['note_1'].fillna('')
        logging.info('note_1 欄位處理完成，移除預設內容 "配送前請先電聯，謝謝！"')
    
    # 處理 note_2 欄位（原款式欄位）
    if 'note_2' in df.columns:
        df['note_2'] = df['note_2'].astype(str)
        df['note_2'] = df['note_2'].replace('nan', '')
        df['note_2'] = df['note_2'].fillna('')
        logging.info('note_2 欄位處理完成')
    
    # 新增平台佣金率欄位：platform_commission_rate
    if 'unit_price' in df.columns and 'reconciliation_amount' in df.columns:
        def calculate_commission_rate(row):
            try:
                unit_price = float(row['unit_price']) if str(row['unit_price']).strip() not in ['', 'nan', 'None', 'NULL', 'null', 'NaN', 'NAN'] else 0.0
                reconciliation = float(row['reconciliation_amount']) if str(row['reconciliation_amount']).strip() not in ['', 'nan', 'None', 'NULL', 'null', 'NaN', 'NAN'] else 0.0
                
                if unit_price > 0:
                    # 計算佣金率：(單價 - 對帳金額) ÷ 單價
                    commission_rate = (unit_price - reconciliation) / unit_price
                    return f'{commission_rate:.4f}'  # 資料庫格式，保留4位小數
                else:
                    return '0.0000'
            except (ValueError, TypeError, ZeroDivisionError):
                return '0.0000'
        
        df['platform_commission_rate'] = df.apply(calculate_commission_rate, axis=1)
        logging.info('新增 platform_commission_rate 欄位完成，計算公式：(unit_price - reconciliation_amount) ÷ unit_price')
    else:
        logging.warning('找不到 unit_price 或 reconciliation_amount 欄位，跳過 platform_commission_rate 計算')
    
    # 新增贈品判斷欄位：is_gift
    if 'product_name_platform' in df.columns and 'unit_price' in df.columns:
        def is_gift_item(row):
            try:
                product_name = str(row['product_name_platform']).strip() if pd.notna(row['product_name_platform']) else ''
                unit_price = float(row['unit_price']) if str(row['unit_price']).strip() not in ['', 'nan', 'None', 'NULL', 'null', 'NaN', 'NAN'] else 0.0
                
                # 判斷條件：商品名稱包含 "-網贈" 且 單價為 0
                if '-網贈' in product_name and unit_price == 0.0:
                    return True
                else:
                    return False
            except (ValueError, TypeError):
                return False
        
        df['is_gift'] = df.apply(is_gift_item, axis=1)
        logging.info('新增 is_gift 欄位完成，判斷條件：product_name_platform 包含 "-網贈" 且 unit_price = 0')
    else:
        logging.warning('找不到 product_name_platform 或 unit_price 欄位，跳過 is_gift 計算')
    
    logging.info('衍生欄位新增完成')
    
    return df


def reorder_columns(df: pd.DataFrame) -> pd.DataFrame:
    """重新排序欄位"""
    logging.info('開始重新排序欄位')
    
    # 定義欄位優先順序 - 按照用戶指定的順序
    priority_columns = [
        'platform', 'order_date', 'order_sn', 'item_no', 'order_line_uid',
        'shipping_carrier', 'shipping_expected_date', 'shipping_expected_time', 'package_count', 'extra_shipping_fee',
        'customer_name', 'shipping_address', 'customer_phone', 'customer_tel_day', 'customer_tel_night', 'note_1', 'note_2',
        'product_sale_id', 'color', 'style', 'product_name_platform', 'quantity', 'unit_price',
        'order_amount', 'reconciliation_amount', 'cost_to_platform', 'platform_commission_rate', 'profit', 'cod_amount', 'invoice_no', 'reconciliation_item_no', 'is_gift'
    ]
    
    # 重新排序欄位
    existing_columns = [col for col in priority_columns if col in df.columns]
    remaining_columns = [col for col in df.columns if col not in existing_columns]
    
    final_columns = existing_columns + sorted(remaining_columns)
    df = df[final_columns]
    
    logging.info(f'欄位重新排序完成，共 {len(df.columns)} 個欄位')
    
    return df


def clean_platform_orders_file(file_path: Path) -> pd.DataFrame:
    """清洗單一大平台貼單檔案，返回清洗後的 DataFrame"""
    try:
        logging.info(f'開始處理檔案：{file_path.name}')
        
        # 讀取 CSV 檔案，指定特定欄位為字串類型
        dtype_dict = {
            '出貨商品編號': str,
            '顏色': str,
            '代收總金額': str,
            '發票編號': str,
            '對帳用品編號': str,
            '提貨人行動電話': str,
            '提貨人日間電話': str,
            '提貨人夜間電話': str
        }
        df = pd.read_csv(file_path, encoding='utf-8-sig', dtype=dtype_dict)
        logging.info(f'原始資料：{len(df)} 行 × {len(df.columns)} 欄')
        
        # 執行清洗步驟
        df = clean_column_names(df)
        df = clean_data_content(df)
        df = remove_invalid_rows(df)
        df = add_derived_fields(df)
        df = reorder_columns(df)
        
        # 資料內容排序：先按 order_date，再按 order_sn
        if 'order_date' in df.columns and 'order_sn' in df.columns:
            # 確保 order_date 是日期格式
            df['order_date'] = pd.to_datetime(df['order_date'], errors='coerce')
            # 排序：先按 order_date（降序，最新的在前），再按 order_sn（升序）
            df = df.sort_values(['order_date', 'order_sn'], ascending=[False, True])
            logging.info(f'資料內容排序完成：按 order_date（降序）> order_sn（升序）')
        elif 'order_sn' in df.columns:
            # 如果沒有 order_date，只按 order_sn 排序
            df = df.sort_values('order_sn', ascending=True)
            logging.info(f'資料內容排序完成：按 order_sn（升序）')
        else:
            logging.warning('找不到 order_date 或 order_sn 欄位，跳過資料排序')
        
        # 最後階段：確保所有欄位的空值都轉換為空白字串
        for col in df.columns:
            if col == 'cost_to_platform':
                # 特別處理 cost_to_platform 欄位，強制轉換為字串類型
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN'], '')
                df[col] = df[col].fillna('')
            elif col in ['customer_phone', 'customer_tel_day', 'customer_tel_night']:
                # 特別處理電話欄位，強制轉換為字串類型
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN'], '')
                df[col] = df[col].fillna('')
            elif col in ['product_sale_id', 'color', 'invoice_no', 'reconciliation_item_no']:
                # 特別處理其他需要字串格式的欄位，強制轉換為字串類型
                df[col] = df[col].astype(str)
                df[col] = df[col].replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN'], '')
                df[col] = df[col].fillna('')
            elif col in ['unit_price', 'order_amount', 'reconciliation_amount', 'cod_amount']:
                # 特別處理價格欄位，確保小數點格式
                def format_price_final(price_value):
                    if pd.isna(price_value) or str(price_value).strip() in ['', 'nan', 'None', 'NULL', 'null', 'NaN', 'NAN']:
                        return '0.00'
                    try:
                        # 轉換為浮點數
                        price_float = float(price_value)
                        # 格式化為兩位小數的字串
                        return f'{price_float:.2f}'
                    except (ValueError, TypeError):
                        return '0.00'
                
                df[col] = df[col].apply(format_price_final)
            elif df[col].dtype == 'object':
                # 字串欄位：將各種空值轉換為空白字串
                df[col] = df[col].replace(['nan', 'None', 'NULL', 'null', 'NaN', 'NAN'], '')
                df[col] = df[col].fillna('')
            else:
                # 數值欄位：將 NaN 轉換為空白字串
                df[col] = df[col].fillna('')
        
        logging.info(f'檔案清洗完成：{file_path.name}')
        logging.info(f'清洗後資料：{len(df)} 行 × {len(df.columns)} 欄')
        
        return df
        
    except Exception as e:
        logging.error(f'處理檔案 {file_path.name} 時發生錯誤：{e}')
        return None


def main() -> None:
    """主函數 - 批次執行並合併資料"""
    setup_logging()
    
    # 取得專案根目錄
    project_root = Path(__file__).resolve().parents[2]
    data_raw_dir = project_root / 'data_raw' / 'etmall'
    temp_dir = project_root / 'temp' / 'etmall'
    
    logging.info(f'專案根目錄：{project_root}')
    logging.info(f'資料來源目錄：{data_raw_dir}')
    logging.info(f'輸出目錄：{temp_dir}')
    
    # 尋找大平台貼單檔案
    platform_files = find_platform_order_files(data_raw_dir)
    
    if not platform_files:
        logging.error('找不到任何大平台貼單檔案，程式結束')
        sys.exit(1)
    
    # 批次處理：先清洗所有檔案，然後合併
    all_cleaned_dfs = []
    success_count = 0
    total_count = len(platform_files)
    
    logging.info(f'\n=== 開始批次處理 {total_count} 個檔案 ===')
    
    # 第一階段：清洗所有檔案
    for i, file_path in enumerate(platform_files, 1):
        logging.info(f'\n--- 清洗檔案 {i}/{total_count}: {file_path.name} ---')
        
        cleaned_df = clean_platform_orders_file(file_path)
        if cleaned_df is not None:
            all_cleaned_dfs.append(cleaned_df)
            success_count += 1
            logging.info(f'檔案 {file_path.name} 清洗成功，資料行數：{len(cleaned_df)}')
        else:
            logging.error(f'檔案 {file_path.name} 清洗失敗')
    
    if not all_cleaned_dfs:
        logging.error('沒有成功清洗任何檔案，程式結束')
        sys.exit(1)
    
    # 第二階段：合併所有清洗後的資料
    logging.info(f'\n=== 開始合併 {len(all_cleaned_dfs)} 個檔案的資料 ===')
    
    try:
        # 合併所有 DataFrame
        merged_df = pd.concat(all_cleaned_dfs, ignore_index=True)
        logging.info(f'資料合併完成，總行數：{len(merged_df)}')
        
        # 生成合併後的輸出檔案名稱
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_filename = f'01_etmall_platform_orders_merged_{timestamp}.csv'
        output_path = temp_dir / output_filename
        
        # 確保輸出目錄存在
        temp_dir.mkdir(parents=True, exist_ok=True)
        
        # 儲存合併後的資料
        merged_df.to_csv(output_path, index=False, encoding='utf-8-sig', na_rep='')
        
        logging.info(f'合併檔案儲存完成：{output_path.name}')
        logging.info(f'最終資料：{len(merged_df)} 行 × {len(merged_df.columns)} 欄')
        
    except Exception as e:
        logging.error(f'資料合併時發生錯誤：{e}')
        sys.exit(1)
    
    # 輸出處理結果
    logging.info(f'\n=== 批次處理完成 ===')
    logging.info(f'成功處理：{success_count}/{total_count} 個檔案')
    logging.info(f'合併後總資料行數：{len(merged_df)}')
    logging.info(f'合併檔案位置：{output_path}')


if __name__ == '__main__':
    main()
