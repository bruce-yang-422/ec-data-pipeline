# scripts/etmall_orders_etl/02_etmall_orders_cleaner.py
"""
東森購物訂單資料合併腳本

功能：
- 讀取由 01_etmall_xlsx_to_csv.py 產生的 CSV 檔案（01_東森購物_*.csv）
- 載入 config/etmall_fields_mapping.json
- 進行基本的文字清理（去除換行符、多餘空白等）
- 新增自定義欄位：platform, order_date, order_line_uid
- 將所有檔案合併為一個 CSV 檔案
- 輸出到 temp/etmall/ 目錄

使用方式：
直接執行此腳本，它會自動處理 data_raw/etmall/ 目錄下所有 01_東森購物_*.csv 檔案，
並將合併後的結果輸出到 temp/etmall/
"""

import pandas as pd
from pathlib import Path
import sys
import re
import json
from datetime import datetime
from typing import Dict, Any, List
import logging

def setup_logging(project_root: Path):
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)

    # 清除舊的日誌檔案
    for log_file in log_dir.glob('etmall_orders_cleaner_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")

    log_filename = f'etmall_orders_cleaner_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, 'w', 'utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_json_config(file_path: Path) -> dict:
    """
    載入 JSON 配置檔案
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"錯誤：找不到配置檔案 {file_path}")
        return {}
    except json.JSONDecodeError as e:
        logging.error(f"錯誤：解析 JSON 檔案失敗 {file_path} - {e}")
        return {}
    except Exception as e:
        logging.exception(f"錯誤：載入配置檔案時發生未知錯誤 {file_path}")
        return {}

def clean_text(text: Any) -> str:
    """
    清理文字，去除特殊符號、換行符號和多餘空白
    """
    if pd.isna(text) or text is None:
        return ""
    
    text = str(text)
    text = text.replace('\n', ' ').replace('\r', ' ').replace('\t', ' ')
    text = re.sub(r'\s+', ' ', text).strip()
    
    # 此處保留所有標點符號，但去除 emoji 和其他非 ASCII/中文的字元
    text = re.sub(r'[\U00010000-\U0010ffff]', '', text)
    
    return text

def clean_datetime_fields(df: pd.DataFrame) -> pd.DataFrame:
    """
    清理日期時間欄位，將包含日期時間的欄位拆分為日期和時間兩個欄位
    """
    df_cleaned = df.copy()
    
    # 定義需要拆分的日期時間欄位
    datetime_fields = [
        'order_date',  # 新增 order_date 欄位處理
        'shipping_request_date',
        'shipping_expected_date',
        'expected_stockin_date',
        'expected_delivery_date'
    ]
    
    # 處理每個日期時間欄位
    for field in datetime_fields:
        if field in df_cleaned.columns:
            logging.info(f'正在拆分 {field} 欄位為日期和時間...')
            
            # 將欄位轉換為字串
            df_cleaned[f'{field}_原始'] = df_cleaned[field].astype(str)
            
            # 嘗試解析日期時間
            try:
                # 處理不同的日期時間格式
                datetime_series = pd.to_datetime(df_cleaned[f'{field}_原始'], errors='coerce')
                
                # 拆分為日期和時間
                df_cleaned[field] = datetime_series.dt.date.astype(str)  # 只要日期部分
                df_cleaned[f'{field}_時間'] = datetime_series.dt.time.astype(str)  # 只要時間部分
                
                # 移除臨時欄位
                df_cleaned = df_cleaned.drop(f'{field}_原始', axis=1)
                
                # 重新排列欄位順序，讓時間欄位緊接在日期欄位後面
                cols = df_cleaned.columns.tolist()
                date_idx = cols.index(field)
                time_idx = cols.index(f'{field}_時間')
                
                # 移除時間欄位從原位置
                cols.pop(time_idx)
                # 移除時間欄位從原位置
                cols.insert(date_idx + 1, f'{field}_時間')
                
                df_cleaned = df_cleaned[cols]
                
                # 特別處理：若為 order_date，將時間補到『下單時間』欄位（僅在其為空時補），並移除臨時時間欄位
                if field == 'order_date':
                    temp_time_col = f'{field}_時間'
                    # 正規化 'NaT' 為空字串
                    if temp_time_col in df_cleaned.columns:
                        df_cleaned[temp_time_col] = df_cleaned[temp_time_col].replace('NaT', '')
                    # 確保存在中文『下單時間』欄位（之後會在最終階段被映射為 order_time）
                    if '下單時間' not in df_cleaned.columns:
                        df_cleaned['下單時間'] = ''
                    # 僅在『下單時間』為空而臨時時間有值時進行補值
                    if temp_time_col in df_cleaned.columns:
                        mask_fill_time = (df_cleaned['下單時間'].astype(str) == '') & \
                                         (df_cleaned[temp_time_col].astype(str) != '') & \
                                         (df_cleaned[temp_time_col].astype(str) != 'NaT')
                        df_cleaned.loc[mask_fill_time, '下單時間'] = df_cleaned.loc[mask_fill_time, temp_time_col]
                        # 不再保留臨時時間欄位
                        df_cleaned = df_cleaned.drop(columns=[temp_time_col])

                logging.info(f'成功拆分 {field} 欄位，共處理 {len(df_cleaned)} 筆資料')
                
            except Exception as e:
                logging.warning(f'{field} 日期時間拆分時發生錯誤：{e}，保留原始格式')
    
    return df_cleaned


def fix_item_no_sequence(df: pd.DataFrame) -> pd.DataFrame:
    """
    檢查並修正 item_no 流水號，確保相同 order_sn 的 item_no 是連續的流水號
    注意：只在真正需要時才重新分配，避免破壞原始資料的正確性
    """
    df_fixed = df.copy()
    
    if 'order_sn' not in df_fixed.columns or 'item_no' not in df_fixed.columns:
        logging.warning('缺少 order_sn 或 item_no 欄位，無法修正流水號')
        return df_fixed
    
    # 按 order_sn 分組，檢查 item_no 的連續性
    logging.info('檢查 item_no 流水號連續性...')
    
    # 將 item_no 轉換為數值型別進行排序
    df_fixed['item_no_numeric'] = pd.to_numeric(df_fixed['item_no'], errors='coerce')
    
    # 檢查每個 order_sn 組內的 item_no 是否已經連續
    needs_fixing = False
    for order_sn, group in df_fixed.groupby('order_sn'):
        group_item_nos = group['item_no_numeric'].dropna().sort_values().tolist()
        if len(group_item_nos) > 1:
            # 檢查是否從1開始且連續
            expected_sequence = list(range(1, len(group_item_nos) + 1))
            if group_item_nos != expected_sequence:
                # 檢查是否為常見的業務情況（例如：item_no 1,3 表示原本就沒有 item_no 2）
                # 如果 item_no 的間隔合理（例如：1,3 或 1,2,4 等），則不強制重新分配
                if len(group_item_nos) >= 2:
                    # 檢查間隔是否合理（允許跳號，但不允許亂序）
                    is_reasonable_gap = True
                    for i in range(1, len(group_item_nos)):
                        if group_item_nos[i] <= group_item_nos[i-1]:
                            is_reasonable_gap = False
                            break
                    
                    if is_reasonable_gap:
                        logging.info(f'訂單 {order_sn} 的 item_no 有跳號但順序正確：{group_item_nos}，這是正常的業務情況，無需重新分配')
                        continue
                    else:
                        needs_fixing = True
                        logging.info(f'訂單 {order_sn} 的 item_no 順序錯誤：{group_item_nos}，需要重新分配')
                        break
                else:
                    needs_fixing = True
                    logging.info(f'訂單 {order_sn} 的 item_no 不連續：{group_item_nos}，期望：{expected_sequence}')
                    break
    
    if needs_fixing:
        logging.info('發現 item_no 流水號不連續，正在重新分配...')
        # 按 order_sn 分組，重新分配 item_no 流水號
        df_fixed = df_fixed.sort_values(['order_sn', 'item_no_numeric']).reset_index(drop=True)
        
        # 為每個 order_sn 重新分配連續的 item_no
        df_fixed['item_no_new'] = df_fixed.groupby('order_sn').cumcount() + 1
        
        # 更新 item_no 欄位
        df_fixed['item_no'] = df_fixed['item_no_new'].astype(str)
        logging.info('item_no 流水號重新分配完成')
        
        # 清理臨時欄位
        df_fixed = df_fixed.drop(['item_no_numeric', 'item_no_new'], axis=1)
    else:
        logging.info('所有 item_no 流水號都是連續的，無需修正')
        # 清理臨時欄位
        df_fixed = df_fixed.drop(['item_no_numeric'], axis=1)
    
    return df_fixed


def convert_columns_to_english(df: pd.DataFrame, mapping_config: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    將 DataFrame 的中文欄位名稱轉換為英文
    """
    df_converted = df.copy()
    
    # 建立中文到英文的欄位名稱映射
    zh_to_en_mapping = {}
    for field_key, field_config in mapping_config.items():
        zh_name = field_config.get('zh_name', '')
        if zh_name:
            zh_to_en_mapping[zh_name] = field_key
    
    # 轉換欄位名稱：中文轉英文
    columns_to_rename = {}
    for col in df_converted.columns:
        if col in zh_to_en_mapping:
            columns_to_rename[col] = zh_to_en_mapping[col]
    
    if columns_to_rename:
        df_converted = df_converted.rename(columns=columns_to_rename)
        logging.info(f"已轉換 {len(columns_to_rename)} 個欄位名稱：{list(columns_to_rename.items())}")
    
    return df_converted


def reorder_columns_by_mapping(df: pd.DataFrame, mapping_config: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    """
    按照 etmall_fields_mapping.json 中定義的順序重新排列 DataFrame 的欄位
    """
    df_reordered = df.copy()
    
    # 從 mapping_config 中提取欄位順序
    ordered_columns = []
    existing_columns = set(df_reordered.columns)
    
    # 按照 order 欄位排序
    sorted_fields = sorted(mapping_config.items(), key=lambda x: int(x[1].get('order', '999')))
    
    for field_key, field_config in sorted_fields:
        # 檢查欄位是否存在於 DataFrame 中
        if field_key in existing_columns:
            ordered_columns.append(field_key)
            existing_columns.remove(field_key)
    
    # 將剩餘的欄位（不在映射配置中的）添加到最後
    remaining_columns = sorted(list(existing_columns))
    ordered_columns.extend(remaining_columns)
    
    # 重新排列 DataFrame 的欄位順序
    df_reordered = df_reordered[ordered_columns]
    
    logging.info(f"欄位重新排列完成，總共 {len(ordered_columns)} 個欄位")
    logging.info(f"欄位順序：{ordered_columns[:10]}{'...' if len(ordered_columns) > 10 else ''}")
    
    return df_reordered


def _map_zh_list_to_en_keys(zh_list: list[str], mapping_config: Dict[str, Dict[str, str]]) -> list[str]:
    """
    將一組中文欄位名映射為英文鍵名（依據 mapping_config 的 zh_name）。
    內建一些常見別名的對應（例如：訂單編號→order_sn、訂單項次→item_no、通路→channel、通路別→channel_type、公司別→company_name）。
    """
    # 反向映射：中文 → 英文鍵
    zh_to_key: Dict[str, str] = {}
    for key, cfg in mapping_config.items():
        zh = str(cfg.get('zh_name', '')).strip()
        if zh:
            zh_to_key[zh] = key

    # 常見別名（原始欄位名）到英文字段鍵的補充映射
    alias_zh_to_key: Dict[str, str] = {
        '訂單編號': 'order_sn',
        '訂單項次': 'item_no',
        '通路': 'channel',  # 購買來源裝置（Mobile / Web 等）
        '通路別': 'channel_type',  # 購買來源平台類別（ETMall、EMALL_APP等）
        '公司別': 'company_name',
        '公司': 'company_name', # 銷售報表中的「公司」
        '商品屬性': 'product_attribute',
        '子商品商品編號': 'sub_product_id',
        '子商品銷售編號': 'sub_sale_id',
        '配送方式': 'shipping_method',
        '配送狀態': 'shipping_status',
        '配送確認日': 'shipping_confirm_date',
        '訂單狀態': 'order_status',
    }

    result_keys: list[str] = []
    for zh in zh_list:
        key = zh_to_key.get(zh)
        if not key:
            # 嘗試別名
            alias_key = alias_zh_to_key.get(zh)
            if alias_key and alias_key in mapping_config:
                key = alias_key
        if key:
            result_keys.append(key)
    return result_keys


def filter_intermediate_columns(df: pd.DataFrame, mapping_config: Dict[str, Dict[str, str]], report_type: str) -> pd.DataFrame:
    """
    依照需求：兩個中間輸出檔（sales_report/detail_report）除了 platform 外，不要產生原本資料沒有的欄位。
    這裡使用使用者指定的原始中文欄位清單做白名單，轉為英文鍵後只保留對應欄位（加上 platform）。
    並按照 mapping_config 的順序重新排列欄位。
    """
    # 使用者指定：
    # - detail_report 輸入（01_東森購物_*.csv）期望欄位（中文）
    detail_report_zh = [
        '訂單號碼','訂單項次','併單序號','送貨單號','銷售編號','商品編號','商品名稱','顏色','款式','廠商商品號碼',
        '訂單類別','數量','售價','成本','客戶名稱','客戶電話','室內電話','配送地址','貨運公司','配送單號',
        '出貨指示日','要求配送日','要求配送時間','備註','贈品','廠商配送訊息','預計入庫日','預計配送日','通路別',
        '訂單類別代號','公司別'
    ]

    # - sales_report 輸入（01_東森購物_銷售報表_*.csv）期望欄位（中文）
    sales_report_zh = [
        '訂單日期','下單時間','訂單編號','項次','配送狀態','訂單狀態','商品屬性','銷售編號','子商品銷售編號','子商品商品編號',
        '配送方式','商品名稱','顏色','款式','售價','成本','數量','通路','配送確認日','公司'
    ]

    if report_type == 'sales_report':
        keep_keys = _map_zh_list_to_en_keys(sales_report_zh, mapping_config)
    else:
        keep_keys = _map_zh_list_to_en_keys(detail_report_zh, mapping_config)

    # 保留欄位：platform + 白名單內目前存在於 df 的欄位
    keep_set = set(keep_keys)
    available_columns = [col for col in df.columns if col in keep_set]
    
    # 確保 order_sn 欄位被保留（因為它是關鍵欄位）
    if 'order_sn' in df.columns and 'order_sn' not in available_columns:
        available_columns.append('order_sn')
        logging.info("已強制保留 order_sn 欄位（關鍵欄位）")
    
    # 按照 mapping_config 的順序重新排列欄位
    ordered_columns = []
    existing_columns = set(available_columns)
    
    # 按照 order 欄位排序
    sorted_fields = sorted(mapping_config.items(), key=lambda x: int(x[1].get('order', '999')))
    
    for field_key, field_config in sorted_fields:
        # 檢查欄位是否存在於過濾後的欄位中
        if field_key in existing_columns:
            ordered_columns.append(field_key)
            existing_columns.remove(field_key)
    
    # 將剩餘的欄位（不在映射配置中的）添加到最後
    remaining_columns = sorted(list(existing_columns))
    ordered_columns.extend(remaining_columns)
    
    # 確保 platform 始終在第一個位置
    final_columns = ['platform']
    for col in ordered_columns:
        if col != 'platform':
            final_columns.append(col)
    
    logging.info(f"中間檔欄位過濾完成，保留 {len(final_columns)} 個欄位")
    logging.info(f"欄位順序：{final_columns[:10]}{'...' if len(final_columns) > 10 else ''}")
    
    return df[final_columns]


def apply_clean_and_transform(df: pd.DataFrame, mapping_config: Dict[str, Dict[str, str]], file_name: str = "") -> pd.DataFrame:
    """
    對 DataFrame 進行基本的文字清理，新增自定義欄位
    欄位名稱轉換會在最後統一處理
    """
    # 對整個 DataFrame 進行空值處理，並將所有 'nan' 字串替換為空字串
    df = df.fillna('').astype(str).replace('nan', '')
    
    # 對所有文字欄位進行基本清理
    for col in df.columns:
        df[col] = df[col].apply(clean_text)
    
    # 新增自定義欄位
    logging.info("新增自定義欄位...")
    
    # 1. 新增 platform 欄位，固定為 'etmall'
    df['platform'] = 'etmall'
    logging.info("已新增 'platform' 欄位，設定為 'etmall'")
    
    # 2. 根據檔案名稱識別資料來源類型
    if file_name and '銷售報表' in file_name:
        df['data_source'] = 'sales_report'
        logging.info(f"根據檔案名稱 '{file_name}' 識別為銷售報表 (sales_report)")
    else:
        df['data_source'] = 'detail_report'
        logging.info(f"根據檔案名稱 '{file_name}' 識別為明細報表 (detail_report)")
    
    # 3. 處理 order_date 欄位，優先從訂單日期擷取，次要從出貨指示日擷取
    if '訂單日期' in df.columns and df['訂單日期'].notna().any():
        # 銷售報表才有訂單日期欄位，使用實際的客戶下單日期
        df['order_date'] = df['訂單日期'].astype(str)
        df = df.drop('訂單日期', axis=1)  # 移除原始欄位避免重複映射
        logging.info("已新增 'order_date' 欄位，來自銷售報表的訂單日期（客戶下單日期）")
        logging.info(f"訂單日期範例：{df['order_date'].iloc[0] if len(df) > 0 else 'N/A'}")
    elif 'order_date' in df.columns and df['order_date'].notna().any():
        # 如果已經有 order_date 欄位且有資料，就不需要再處理
        logging.info("已存在 'order_date' 欄位，跳過新增")
    elif '出貨指示日' in df.columns and df['出貨指示日'].notna().any():
        # 明細報表沒有訂單日期，從出貨指示日擷取
        df['order_date'] = df['出貨指示日'].astype(str)
        logging.info("已新增 'order_date' 欄位，來自明細報表的出貨指示日")
        logging.info(f"訂單日期範例：{df['order_date'].iloc[0] if len(df) > 0 else 'N/A'}")
    else:
        # 沒有任何可用的日期欄位，使用預設值
        df['order_date'] = '1900-01-01'
        logging.warning("無法擷取訂單日期，order_date 設為預設值 '1900-01-01'")
        logging.info("預設值 '1900-01-01' 是國際通用的缺失日期標準值")
        logging.info("建議使用銷售報表（包含訂單日期欄位）以獲得準確的客戶下單日期")
    
    # 3. 統一欄位名稱：將 '訂單編號' 重命名為 '訂單號碼'（銷售報表統一化）
    if '訂單編號' in df.columns:
        df['訂單號碼'] = df['訂單編號']
        df = df.drop('訂單編號', axis=1)
        logging.info("已將 '訂單編號' 重命名為 '訂單號碼'（統一欄位名稱）")
    
    # 3.1 將 '訂單號碼' 映射為 'order_sn'，確保後續處理有正確的欄位名稱
    if '訂單號碼' in df.columns:
        df['order_sn'] = df['訂單號碼']
        logging.info("已將 '訂單號碼' 映射為 'order_sn'，確保後續處理有正確的欄位名稱")

    # 3.1 通路欄位處理：保持 '通路' 和 '通路別' 分別映射到 'channel' 和 'channel_type'
    # 不再進行欄位轉換，讓兩個欄位分別處理
    if '通路' in df.columns:
        logging.info("發現 '通路' 欄位，將映射到 'channel'（購買來源裝置）")
    if '通路別' in df.columns:
        logging.info("發現 '通路別' 欄位，將映射到 'channel_type'（購買來源平台類別）")
    
    # 4. 新增 order_line_uid 欄位，使用 order_sn + item_no 作為唯一 key
    # 處理不同的項次欄位名稱：銷售報表用「項次」，明細報表用「訂單項次」
    if '訂單號碼' in df.columns:
        # 檢查是否有項次欄位（銷售報表）
        if '項次' in df.columns:
            df['order_line_uid'] = df['訂單號碼'].astype(str) + '-' + df['項次'].astype(str)
            logging.info("已新增 'order_line_uid' 欄位，使用 order_sn + 項次 作為唯一 key")
        # 檢查是否有訂單項次欄位（明細報表）
        elif '訂單項次' in df.columns:
            df['order_line_uid'] = df['訂單號碼'].astype(str) + '-' + df['訂單項次'].astype(str)
            # 將訂單項次的資料複製到項次欄位，然後移除原始欄位
            # 這樣在欄位映射階段，項次會被正確映射到 item_no
            df['項次'] = df['訂單項次']
            df = df.drop('訂單項次', axis=1)
            logging.info("已新增 'order_line_uid' 欄位，使用 order_sn + 訂單項次 作為唯一 key")
            logging.info("已將 '訂單項次' 資料複製到 '項次' 欄位並移除原始欄位")
        else:
            df['order_line_uid'] = ''
            logging.warning("找不到 '項次' 或 '訂單項次' 欄位，order_line_uid 設為空值")
    else:
        df['order_line_uid'] = ''
        logging.warning("找不到 '訂單號碼' 欄位，order_line_uid 設為空值")
    
    return df

def main():
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)
    
    # 設定輸入與輸出目錄
    input_dir = project_root / 'data_raw' / 'etmall'
    # 輸出目錄
    output_dir = project_root / 'temp' / 'etmall'
    # 欄位映射檔案
    fields_mapping_file = project_root / 'config' / 'etmall_fields_mapping.json'
    
    # 檢查目錄是否存在
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f'錯誤：找不到輸入目錄 {input_dir}')
        sys.exit(1)
    
    if not output_dir.exists():
        output_dir.mkdir(parents=True)
    
    # 檢查欄位映射檔案是否存在
    if not fields_mapping_file.exists():
        logging.error(f'錯誤：找不到欄位映射檔案 {fields_mapping_file}')
        sys.exit(1)
    
    # 載入欄位映射配置
    logging.info("載入欄位映射配置...")
    mapping_config = load_json_config(fields_mapping_file)
    if not mapping_config:
        logging.error('無法載入欄位映射配置，停止執行')
        sys.exit(1)
    logging.info(f"已載入欄位映射配置，包含 {len(mapping_config)} 個欄位定義")
    
    # 在開始處理前，清除舊的中間檔檔案
    logging.info(f'清除舊的中間檔檔案...')
    for old_file in output_dir.glob('02_etmall_orders_*.csv'):
        try:
            old_file.unlink()
            logging.info(f'已刪除舊檔案：{old_file.name}')
        except OSError as e:
            logging.error(f"錯誤: 無法刪除舊中間檔檔案 {old_file.name} - {e}")
            
    logging.info(f'讀取目錄：{input_dir}')
    logging.info(f'輸出目錄：{output_dir}')
    logging.info(f'欄位映射檔案：{fields_mapping_file}')
    
    # 搜尋所有 etmall 相關的 CSV 檔案
    logging.info(f'\n=== 搜尋需要清理的 CSV 檔案 ===')
    
    # 分別搜尋兩種格式的檔案
    sales_report_files = []
    detail_report_files = []
    
    try:
        # 搜尋銷售報表檔案（包含「銷售報表」的檔案）
        sales_report_files.extend(input_dir.glob('01_東森購物_銷售報表_*.csv'))
        # 搜尋明細報表檔案（不包含「銷售報表」的檔案）
        detail_report_files.extend(input_dir.glob('01_東森購物_*.csv'))
        # 從明細報表中移除銷售報表檔案（避免重複）
        detail_report_files = [f for f in detail_report_files if '銷售報表' not in f.name]
        
    except Exception as e:
        logging.exception(f'錯誤：搜尋檔案時發生錯誤')
        sys.exit(1)
        
    if not sales_report_files and not detail_report_files:
        logging.warning(f'在 {input_dir} 目錄下沒有找到任何需要清理的 CSV 檔案')
        return
        
    logging.info(f'找到銷售報表檔案 {len(sales_report_files)} 個：')
    for file in sales_report_files:
        logging.info(f'  - {file.name}')
        
    logging.info(f'找到明細報表檔案 {len(detail_report_files)} 個：')
    for file in detail_report_files:
        logging.info(f'  - {file.name}')
    
    logging.info(f'\n=== 開始分別處理兩種報表 ===')
    
    # 分別處理銷售報表和明細報表
    sales_report_dfs = []
    detail_report_dfs = []
    
    # 處理銷售報表檔案
    if sales_report_files:
        logging.info(f'\n=== 處理銷售報表檔案 ===')
        for csv_file in sales_report_files:
            logging.info(f'處理銷售報表檔案：{csv_file.name}')
            try:
                df = pd.read_csv(csv_file, dtype=str)
                df_cleaned = apply_clean_and_transform(df, mapping_config, csv_file.name)
                df_cleaned = clean_datetime_fields(df_cleaned)
                sales_report_dfs.append(df_cleaned)
                logging.info(f'銷售報表檔案 {csv_file.name} 清理完成')
            except Exception as e:
                logging.exception(f'錯誤：清理銷售報表檔案 {csv_file.name} 失敗')
                continue
    
    # 處理明細報表檔案
    if detail_report_files:
        logging.info(f'\n=== 處理明細報表檔案 ===')
        for csv_file in detail_report_files:
            logging.info(f'處理明細報表檔案：{csv_file.name}')
            try:
                df = pd.read_csv(csv_file, dtype=str)
                df_cleaned = apply_clean_and_transform(df, mapping_config, csv_file.name)
                df_cleaned = clean_datetime_fields(df_cleaned)
                detail_report_dfs.append(df_cleaned)
                logging.info(f'明細報表檔案 {csv_file.name} 清理完成')
            except Exception as e:
                logging.exception(f'錯誤：清理明細報表檔案 {csv_file.name} 失敗')
                continue
            
    if not sales_report_dfs and not detail_report_dfs:
        logging.error('沒有成功清理任何檔案，停止執行')
        return
    
    # 分別合併銷售報表和明細報表
    sales_report_final = None
    detail_report_final = None
    
    if sales_report_dfs:
        sales_report_final = pd.concat(sales_report_dfs, ignore_index=True)
        logging.info(f'銷售報表合併完成，總共 {len(sales_report_final)} 筆資料')
        
        # 檢查並修正 item_no 流水號（注意：此時欄位名稱還是中文）
        if 'order_sn' in sales_report_final.columns and '項次' in sales_report_final.columns:
            logging.info('檢查銷售報表 item_no 流水號...')
            sales_report_final = fix_item_no_sequence(sales_report_final)
    
    if detail_report_dfs:
        detail_report_final = pd.concat(detail_report_dfs, ignore_index=True)
        logging.info(f'明細報表合併完成，總共 {len(detail_report_final)} 筆資料')
        
        # 檢查並修正 item_no 流水號（注意：此時欄位名稱還是中文）
        if 'order_sn' in detail_report_final.columns and '項次' in detail_report_final.columns:
            logging.info('檢查明細報表 item_no 流水號...')
            detail_report_final = fix_item_no_sequence(detail_report_final)
    
        # 在合併前，先將兩個檔案的欄位都轉換為英文
    logging.info(f'\n=== 合併前欄位名稱轉換 ===')
    
    if sales_report_final is not None:
        sales_report_final = convert_columns_to_english(sales_report_final, mapping_config)
        logging.info('銷售報表欄位已轉換為英文')
    
    if detail_report_final is not None:
        detail_report_final = convert_columns_to_english(detail_report_final, mapping_config)
        logging.info('明細報表欄位已轉換為英文')
    
    # 在合併前，先將兩個檔案的欄位都按照 mapping_config 重新排序
    logging.info(f'\n=== 合併前欄位重新排序 ===')
    
    if sales_report_final is not None:
        sales_report_final = reorder_columns_by_mapping(sales_report_final, mapping_config)
        logging.info('銷售報表欄位已重新排序')
    
    if detail_report_final is not None:
        detail_report_final = reorder_columns_by_mapping(detail_report_final, mapping_config)
        logging.info('明細報表欄位已重新排序')
        
    # 依需求過濾中間輸出檔欄位（除了 platform 外，不要產生原本資料沒有的欄位）
    logging.info('\n=== 中間檔欄位白名單過濾 ===')
    sales_report_output = None
    detail_report_output = None
    if sales_report_final is not None:
        sales_report_output = filter_intermediate_columns(sales_report_final, mapping_config, 'sales_report')
        logging.info('銷售報表中間檔欄位已套用白名單過濾')
    if detail_report_final is not None:
        detail_report_output = filter_intermediate_columns(detail_report_final, mapping_config, 'detail_report')
        logging.info('明細報表中間檔欄位已套用白名單過濾')
    
    # 儲存中間檔檔案
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # 分別輸出銷售報表和明細報表檔案
    if sales_report_final is not None:
        sales_output_filename = f'02_etmall_orders_sales_report_{timestamp}.csv'
        sales_output_path = output_dir / sales_output_filename
        (sales_report_output if 'sales_report_output' in locals() and sales_report_output is not None else sales_report_final).to_csv(sales_output_path, index=False, encoding='utf-8-sig')
        logging.info(f'已輸出銷售報表檔案：{sales_output_path}')
    
    if detail_report_final is not None:
        detail_output_filename = f'02_etmall_orders_detail_report_{timestamp}.csv'
        detail_output_path = output_dir / detail_output_filename
        (detail_report_output if 'detail_report_output' in locals() and detail_report_output is not None else detail_report_final).to_csv(detail_output_path, index=False, encoding='utf-8-sig')
        logging.info(f'已輸出明細報表檔案：{detail_output_path}')
    
    logging.info(f'\n=== 清理完成 ===')
    logging.info(f'已輸出兩個中間檔：銷售報表和明細報表')
    logging.info(f'檔案名稱反映了資料來源類型')


if __name__ == '__main__':
    main()
