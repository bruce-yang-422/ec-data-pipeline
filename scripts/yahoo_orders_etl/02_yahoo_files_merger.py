#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Yahoo 訂單檔案合併腳本
負責將 data_raw/Yahoo 下不同類型的檔案分別合併，輸出到 temp/Yahoo
參考 config/yahoo_fields_mapping.json 的欄位結構

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import sys
import json
import logging
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import shutil

# 設定路徑
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent.parent
RAW_DIR = PROJECT_ROOT / "data_raw" / "Yahoo"
TEMP_DIR = PROJECT_ROOT / "temp" / "Yahoo"
CONFIG_DIR = PROJECT_ROOT / "config"
LOGS_DIR = PROJECT_ROOT / "logs"

# 確保目錄存在
TEMP_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# 設定日誌
def setup_logging():
    """設定日誌"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = LOGS_DIR / f"yahoo_merger_{timestamp}.log"
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_filename, encoding='utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)

def load_yahoo_fields_mapping() -> Dict:
    """載入 Yahoo 欄位映射配置"""
    try:
        config_file = CONFIG_DIR / "yahoo_fields_mapping.json"
        if not config_file.exists():
            raise FileNotFoundError(f"找不到配置檔案：{config_file}")
        
        with open(config_file, 'r', encoding='utf-8') as f:
            mapping = json.load(f)
        
        logging.info(f"成功載入 Yahoo 欄位映射配置，共 {len(mapping)} 個欄位")
        return mapping
        
    except Exception as e:
        logging.error(f"載入 Yahoo 欄位映射配置失敗：{e}")
        raise

def get_required_fields(mapping: Dict) -> List[str]:
    """獲取必填欄位列表"""
    required_fields = []
    for field_name, field_info in mapping.items():
        if field_info.get('required') == '是':
            required_fields.append(field_name)
    return required_fields

def detect_file_type_from_content(file_path: Path) -> str:
    """根據檔案內容偵測檔案類型（與重新命名腳本保持一致）"""
    try:
        # 嘗試不同的編碼
        encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
        df = None
        
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, dtype=str, encoding=encoding, nrows=5)
                break
            except UnicodeDecodeError:
                continue
            except Exception as e:
                logging.warning(f"編碼 {encoding} 讀取失敗：{e}")
                continue
        
        if df is None or df.empty:
            logging.error(f"無法讀取檔案：{file_path}")
            return 'unknown'
        
        # 將欄位名稱轉換為小寫字串進行比較
        columns = [str(col).lower() for col in df.columns]
        
        # 首先嘗試從檔案名稱判斷（優先使用檔案名稱，因為某些檔案類型欄位結構相似）
        filename_lower = file_path.name.lower()
        if 'spstorders' in filename_lower or 'sps_orders' in filename_lower:
            return 'sps_orders'
        elif 'retgood' in filename_lower:
            return 'retgood'
        elif 'delivery' in filename_lower:
            return 'delivery'
        elif 'torders' in filename_lower or 'orders' in filename_lower:
            return 'orders'
        
        # 如果檔案名稱無法判斷，則根據欄位內容判斷
        # 檢查是否有退貨相關欄位（優先檢查，因為 retgood 檔案可能也有其他欄位）
        elif '退貨單號' in columns or '退貨單序號' in columns:
            return 'retgood'
        
        # 檢查是否有超商相關欄位
        elif '超商類型' in columns:
            return 'sps_orders'
        
        # 檢查是否有收件人相關欄位（delivery 檔案）
        elif '收件人姓名' in columns and '收件人地址' in columns:
            return 'delivery'
        
        # 檢查是否有訂單相關欄位（orders 檔案）
        elif '訂單編號' in columns and '商品名稱' in columns:
            return 'orders'
        
        # 如果都無法判斷，記錄欄位資訊並返回 unknown
        logging.warning(f"無法判斷檔案類型，欄位：{columns}")
        return 'unknown'
            
    except Exception as e:
        logging.error(f"偵測檔案類型時發生錯誤：{e}")
        return 'unknown'

def create_zh_to_en_mapping(mapping: Dict) -> Dict[str, str]:
    """創建中文欄位名稱到英文欄位名稱的映射"""
    zh_to_en = {}
    for field_name, field_info in mapping.items():
        zh_name = field_info.get('zh_name', '')
        if zh_name:
            zh_to_en[zh_name] = field_name
    return zh_to_en

def map_chinese_columns_to_english(df: pd.DataFrame, zh_to_en_mapping: Dict[str, str]) -> pd.DataFrame:
    """將中文欄位名稱映射為英文欄位名稱"""
    try:
        # 創建欄位名稱映射
        column_mapping = {}
        for col in df.columns:
            col_str = str(col)
            if col_str in zh_to_en_mapping:
                column_mapping[col_str] = zh_to_en_mapping[col_str]
                logging.info(f"欄位映射：{col_str} -> {zh_to_en_mapping[col_str]}")
            else:
                # 如果找不到對應的英文名稱，保持原樣
                column_mapping[col_str] = col_str
                logging.warning(f"找不到欄位 {col_str} 的英文對應，保持原樣")
        
        # 重新命名欄位
        df_mapped = df.rename(columns=column_mapping)
        logging.info(f"欄位映射完成，共 {len(df_mapped.columns)} 個欄位")
        
        return df_mapped
        
    except Exception as e:
        logging.error(f"欄位映射時發生錯誤：{e}")
        return df

def generate_line_numbers_by_group(df: pd.DataFrame) -> List[str]:
    """根據業務邏輯生成序號：同日期 + 同recipient_name + order_sn連號 為一個群"""
    try:
        logging.info(f"開始生成序號，DataFrame 形狀：{df.shape}")
        logging.info(f"DataFrame 欄位：{list(df.columns)}")
        
        # 檢查必要欄位是否存在
        required_fields = ['order_date', 'recipient_name', 'order_sn']
        for field in required_fields:
            if field not in df.columns:
                logging.error(f"缺少必要欄位：{field}")
                return [''] * len(df)
        
        # 創建一個字典來存儲每行的序號
        line_number_dict = {}
        current_group = 1
        
        # 按日期、收件人姓名、訂單編號排序，但保留原始索引
        df_sorted = df.sort_values(['order_date', 'recipient_name', 'order_sn'])
        
        prev_date = None
        prev_recipient = None
        prev_order_sn = None
        
        for idx, row in df_sorted.iterrows():
            current_date = row['order_date']
            current_recipient = row['recipient_name']
            current_order_sn = row['order_sn']
            
            # 判斷是否為新群組
            is_new_group = False
            
            # 檢查日期是否相同
            if current_date != prev_date:
                is_new_group = True
                logging.debug(f"新群組：日期不同 {prev_date} -> {current_date}")
            # 檢查收件人是否相同
            elif current_recipient != prev_recipient:
                is_new_group = True
                logging.debug(f"新群組：收件人不同 {prev_recipient} -> {current_recipient}")
            # 檢查訂單編號是否連號
            elif prev_order_sn and current_order_sn:
                try:
                    # 提取訂單編號的數字部分進行比較
                    prev_num = int(str(prev_order_sn).replace('RM', ''))
                    current_num = int(str(current_order_sn).replace('RM', ''))
                    if current_num != prev_num + 1:
                        is_new_group = True
                        logging.debug(f"新群組：訂單編號不連號 {prev_num} -> {current_num}")
                except (ValueError, AttributeError) as e:
                    # 如果無法解析為數字，視為新群組
                    is_new_group = True
                    logging.debug(f"新群組：無法解析訂單編號 {prev_order_sn} -> {current_order_sn}, 錯誤：{e}")
            
            # 如果是新群組，重置序號
            if is_new_group:
                current_group = 1
                logging.debug(f"重置序號為 1")
            else:
                current_group += 1
                logging.debug(f"序號遞增為 {current_group}")
            
            # 直接使用原始索引
            line_number_dict[idx] = str(current_group)
            
            # 更新前一個值
            prev_date = current_date
            prev_recipient = current_recipient
            prev_order_sn = current_order_sn
        
        # 按照原始 DataFrame 的順序返回序號
        line_numbers = []
        for idx in df.index:
            if idx in line_number_dict:
                line_numbers.append(line_number_dict[idx])
            else:
                line_numbers.append('')
                logging.warning(f"索引 {idx} 沒有對應的序號")
        
        logging.info(f"根據業務邏輯生成序號完成，共 {len(line_numbers)} 個序號")
        return line_numbers
        
    except Exception as e:
        logging.error(f"生成序號時發生錯誤：{e}")
        import traceback
        logging.error(f"錯誤詳情：{traceback.format_exc()}")
        # 發生錯誤時返回空值
        return [''] * len(df)

def get_smart_default_value(field_name: str, field_info: Dict, df: pd.DataFrame) -> any:
    """根據欄位描述和備註智能生成預設值"""
    try:
        description = field_info.get('description', '').lower()
        note = field_info.get('note', '').lower()
        field_type = field_info.get('type', '').lower()
        
        # 只處理三個特定欄位的預設值，其他欄位保持空白
        
        # 1. platform 欄位
        if field_name == 'platform':
            # 根據 note 說明：系統自動填入
            return 'yahoo'
        
        # 2. line_number 欄位
        elif field_name == 'line_number':
            # 根據 note 說明：例：1 2 3
            # 同一日同訂購人資料序號，用於識別是否同一筆訂單及項次記錄
            # 需要根據業務邏輯生成，這裡先返回 None，在 standardize_columns 中處理
            return None
        
        # 3. order_date 欄位
        elif field_name == 'order_date':
            # 根據 note 說明：例：RM2506210008426→2025/06/21
            # 從訂單編號前6碼解析的訂單日期(YYMMDD)
            # 需要根據業務邏輯生成，這裡先返回 None，在 standardize_columns 中處理
            return None
        
        # 4. 其他所有欄位都保持空白
        return ''
            
    except Exception as e:
        logging.warning(f"預設值處理失敗，欄位：{field_name}，錯誤：{e}")
        return ''

def standardize_columns(df: pd.DataFrame, file_type: str) -> pd.DataFrame:
    """標準化欄位名稱和順序"""
    try:
        # 載入欄位映射
        mapping = load_yahoo_fields_mapping()
        
        # 獲取欄位順序
        field_order = sorted(mapping.items(), key=lambda x: int(x[1]['order']))
        ordered_fields = [field_name for field_name, _ in field_order]
        
        logging.info(f"開始標準化欄位，順序：{ordered_fields}")
        logging.info(f"原始 DataFrame 欄位：{list(df.columns)}")
        
        # 創建標準化的 DataFrame
        standardized_df = pd.DataFrame()
        
        # 第一階段：處理基本欄位（platform, order_date 等）
        for field_name in ordered_fields:
            if field_name in df.columns:
                # 欄位存在，直接複製
                standardized_df[field_name] = df[field_name]
                logging.info(f"欄位 {field_name} 直接複製，共 {len(df)} 行")
            else:
                # 欄位不存在，使用智能預設值
                field_info = mapping[field_name]
                default_value = get_smart_default_value(field_name, field_info, df)
                
                if field_name == 'order_date':
                    # 從訂單編號解析日期
                    order_dates = []
                    if 'order_sn' in df.columns:
                        for order_sn in df['order_sn']:
                            try:
                                # 從訂單編號前6碼解析日期 (YYMMDD)
                                if order_sn and len(str(order_sn)) >= 6:
                                    date_part = str(order_sn)[2:8]  # 跳過 "RM"，取 6 位數字
                                    if len(date_part) == 6 and date_part.isdigit():
                                        year = "20" + date_part[:2]  # YY -> 20YY
                                        month = date_part[2:4]       # MM
                                        day = date_part[4:6]         # DD
                                        order_date = f"{year}/{month}/{day}"
                                        order_dates.append(order_date)
                                    else:
                                        order_dates.append('')
                                else:
                                    order_dates.append('')
                            except Exception as e:
                                logging.warning(f"解析訂單編號 {order_sn} 日期失敗：{e}")
                                order_dates.append('')
                    else:
                        # 如果沒有訂單編號欄位，填入空值
                        order_dates = [''] * len(df)
                        logging.warning(f"找不到 order_sn 欄位，order_date 填入空值")
                    
                    standardized_df[field_name] = order_dates
                    logging.info(f"欄位 {field_name} 從訂單編號解析日期，共 {len(df)} 行")
                
                elif field_name == 'platform':
                    # platform 欄位直接填入預設值
                    standardized_df[field_name] = [default_value] * len(df)
                    logging.info(f"欄位 {field_name} 填入預設值：{default_value}，共 {len(df)} 行")
                
                else:
                    # 其他欄位暫時填入空值，稍後處理
                    standardized_df[field_name] = [''] * len(df)
                    logging.info(f"欄位 {field_name} 暫時填入空值，稍後處理")
        
        # 第二階段：處理依賴欄位（line_number）
        for field_name in ordered_fields:
            if field_name == 'line_number':
                # 檢查必要欄位是否存在
                required_fields = ['order_date', 'recipient_name', 'order_sn']
                missing_fields = [field for field in required_fields if field not in standardized_df.columns]
                
                if missing_fields:
                    logging.warning(f"缺少必要欄位 {missing_fields}，line_number 填入空值")
                    standardized_df[field_name] = [''] * len(df)
                else:
                    # 檢查欄位是否有值
                    has_data = True
                    for field in required_fields:
                        if standardized_df[field].isna().all() or (standardized_df[field] == '').all():
                            has_data = False
                            logging.warning(f"欄位 {field} 沒有有效資料")
                            break
                    
                    if has_data:
                        # 創建臨時 DataFrame，包含所需的欄位
                        temp_df = pd.DataFrame({
                            'order_date': standardized_df['order_date'],
                            'recipient_name': standardized_df['recipient_name'],
                            'order_sn': standardized_df['order_sn']
                        })
                        
                        # 根據業務邏輯分群
                        line_numbers = generate_line_numbers_by_group(temp_df)
                        standardized_df[field_name] = line_numbers
                        logging.info(f"欄位 {field_name} 根據業務邏輯生成序號，共 {len(df)} 行")
                    else:
                        logging.warning(f"必要欄位沒有有效資料，line_number 填入空值")
                        standardized_df[field_name] = [''] * len(df)
        
        # 添加處理日期欄位
        standardized_df['processing_date'] = [datetime.now()] * len(df)
        
        logging.info(f"標準化完成，共 {len(standardized_df.columns)} 個欄位，{len(standardized_df)} 行資料")
        logging.info(f"最終欄位：{list(standardized_df.columns)}")
        return standardized_df
        
    except Exception as e:
        logging.error(f"標準化欄位時發生錯誤：{e}")
        import traceback
        logging.error(f"錯誤詳情：{traceback.format_exc()}")
        return df

def merge_files_by_type(file_type: str, files: List[Path], logger: logging.Logger, zh_to_en_mapping: Dict[str, str]) -> Optional[pd.DataFrame]:
    """合併指定類型的檔案"""
    try:
        if not files:
            logger.warning(f"沒有找到 {file_type} 類型的檔案")
            return None
        
        logger.info(f"開始合併 {file_type} 類型檔案，共 {len(files)} 個檔案")
        
        merged_data = []
        
        for file_path in files:
            try:
                logger.info(f"處理檔案：{file_path.name}")
                
                # 嘗試不同的編碼
                encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, dtype=str, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        logger.warning(f"編碼 {encoding} 讀取失敗：{e}")
                        continue
                
                if df is None:
                    logger.error(f"無法讀取檔案：{file_path}")
                    continue
                
                # 將中文欄位名稱映射為英文
                df_mapped = map_chinese_columns_to_english(df, zh_to_en_mapping)
                
                # 標準化欄位
                df_standardized = standardize_columns(df_mapped, file_type)
                
                # 添加來源檔案資訊
                df_standardized['source_file'] = file_path.name
                
                merged_data.append(df_standardized)
                logger.info(f"成功處理檔案：{file_path.name}，共 {len(df_standardized)} 筆資料")
                
            except Exception as e:
                logger.error(f"處理檔案 {file_path} 時發生錯誤：{e}")
                continue
        
        if not merged_data:
            logger.warning(f"沒有成功處理的 {file_type} 檔案")
            return None
        
        # 合併所有資料
        final_df = pd.concat(merged_data, ignore_index=True)
        logger.info(f"合併完成，{file_type} 類型總共 {len(final_df)} 筆資料")
        
        # 按照 order_sn 遞增順序排序
        if 'order_sn' in final_df.columns:
            try:
                # 提取 order_sn 的數字部分進行排序
                final_df['order_sn_numeric'] = final_df['order_sn'].apply(
                    lambda x: int(str(x).replace('RM', '')) if x and str(x).startswith('RM') else 0
                )
                final_df = final_df.sort_values('order_sn_numeric', ascending=True)
                final_df = final_df.drop('order_sn_numeric', axis=1)
                logger.info(f"已按照 order_sn 遞增順序排序")
            except Exception as e:
                logger.warning(f"排序失敗，保持原始順序：{e}")
        else:
            logger.warning("找不到 order_sn 欄位，無法排序")
        
        return final_df
        
    except Exception as e:
        logger.error(f"合併 {file_type} 類型檔案時發生錯誤：{e}")
        return None



def deduplicate_data(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """去重資料，基於訂單編號和商品編號的組合"""
    try:
        if df is None or df.empty:
            logger.warning("DataFrame 為空，無法去重")
            return df
        
        original_count = len(df)
        logger.info(f"開始去重處理，原始資料筆數：{original_count}")
        
        # 檢查必要的去重欄位
        dedup_columns = []
        
        # 主要去重欄位：訂單編號 + 商品編號
        if 'order_sn' in df.columns:
            dedup_columns.append('order_sn')
        else:
            logger.warning("找不到 order_sn 欄位，去重效果可能不佳")
        
        if 'product_id' in df.columns:
            dedup_columns.append('product_id')
        else:
            logger.warning("找不到 product_id 欄位，去重效果可能不佳")
        
        # 輔助去重欄位：收件人姓名和商品名稱
        if 'recipient_name' in df.columns:
            dedup_columns.append('recipient_name')
        
        if 'product_name' in df.columns:
            dedup_columns.append('product_name')
        
        if not dedup_columns:
            logger.error("沒有可用的去重欄位，跳過去重處理")
            return df
        
        logger.info(f"使用欄位進行去重：{dedup_columns}")
        
        # 在去重前，記錄重複資料的詳細資訊
        if len(dedup_columns) >= 2:
            # 檢查重複資料
            duplicated_mask = df.duplicated(subset=dedup_columns, keep=False)
            duplicated_df = df[duplicated_mask]
            
            if not duplicated_df.empty:
                logger.warning(f"發現 {len(duplicated_df)} 筆重複資料")
                
                # 統計每個重複組合的數量
                duplicate_groups = duplicated_df.groupby(dedup_columns).size().reset_index(name='count')
                duplicate_groups = duplicate_groups[duplicate_groups['count'] > 1]
                
                for _, group in duplicate_groups.iterrows():
                    group_info = ", ".join([f"{col}={group[col]}" for col in dedup_columns])
                    logger.info(f"重複組合：{group_info} (重複 {group['count']} 次)")
        
        # 執行去重，保留第一筆記錄
        df_deduped = df.drop_duplicates(subset=dedup_columns, keep='first')
        
        duplicated_count = original_count - len(df_deduped)
        logger.info(f"去重完成，移除 {duplicated_count} 筆重複資料，剩餘 {len(df_deduped)} 筆")
        
        if duplicated_count > 0:
            logger.info(f"去重率：{duplicated_count/original_count*100:.2f}%")
        
        return df_deduped
        
    except Exception as e:
        logger.error(f"去重處理時發生錯誤：{e}")
        import traceback
        logger.error(f"錯誤詳情：{traceback.format_exc()}")
        return df

def merge_all_files(files: List[Path], logger: logging.Logger, zh_to_en_mapping: Dict[str, str]) -> Optional[pd.DataFrame]:
    """合併所有檔案，不分類型，並進行去重處理"""
    try:
        if not files:
            logger.warning("沒有找到檔案")
            return None
        
        logger.info(f"開始合併所有檔案，共 {len(files)} 個檔案")
        
        merged_data = []
        
        for file_path in files:
            try:
                logger.info(f"處理檔案：{file_path.name}")
                
                # 偵測檔案類型
                file_type = detect_file_type_from_content(file_path)
                logger.info(f"檔案類型：{file_type}")
                
                # 嘗試不同的編碼
                encodings = ['utf-8-sig', 'utf-8', 'cp950', 'big5', 'gbk', 'gb2312']
                df = None
                
                for encoding in encodings:
                    try:
                        df = pd.read_csv(file_path, dtype=str, encoding=encoding)
                        break
                    except UnicodeDecodeError:
                        continue
                    except Exception as e:
                        logger.warning(f"編碼 {encoding} 讀取失敗：{e}")
                        continue
                
                if df is None:
                    logger.error(f"無法讀取檔案：{file_path}")
                    continue
                
                if df.empty:
                    logger.warning(f"檔案 {file_path.name} 為空檔案，跳過處理")
                    continue
                
                logger.info(f"成功讀取檔案：{file_path.name}，原始資料 {len(df)} 筆")
                
                # 將中文欄位名稱映射為英文
                df_mapped = map_chinese_columns_to_english(df, zh_to_en_mapping)
                
                # 標準化欄位（使用 'orders' 作為預設類型，因為它包含最完整的欄位）
                df_standardized = standardize_columns(df_mapped, file_type)
                
                # 添加來源檔案資訊和檔案類型
                df_standardized['source_file'] = file_path.name
                df_standardized['file_type'] = file_type
                
                merged_data.append(df_standardized)
                logger.info(f"成功處理檔案：{file_path.name}，標準化後 {len(df_standardized)} 筆資料")
                
            except Exception as e:
                logger.error(f"處理檔案 {file_path} 時發生錯誤：{e}")
                import traceback
                logger.error(f"錯誤詳情：{traceback.format_exc()}")
                continue
        
        if not merged_data:
            logger.warning("沒有成功處理的檔案")
            return None
        
        # 合併所有資料
        final_df = pd.concat(merged_data, ignore_index=True)
        logger.info(f"初步合併完成，總共 {len(final_df)} 筆資料")
        
        # 執行去重處理
        final_df = deduplicate_data(final_df, logger)
        
        # 按照 order_sn 遞增順序排序
        if 'order_sn' in final_df.columns:
            try:
                # 過濾出有效的 order_sn
                valid_order_mask = final_df['order_sn'].notna() & (final_df['order_sn'] != '')
                valid_df = final_df[valid_order_mask]
                invalid_df = final_df[~valid_order_mask]
                
                if not valid_df.empty:
                    # 提取 order_sn 的數字部分進行排序
                    valid_df['order_sn_numeric'] = valid_df['order_sn'].apply(
                        lambda x: int(str(x).replace('RM', '')) if x and str(x).startswith('RM') and str(x)[2:].isdigit() else 0
                    )
                    valid_df = valid_df.sort_values('order_sn_numeric', ascending=True)
                    valid_df = valid_df.drop('order_sn_numeric', axis=1)
                    
                    # 將排序後的有效資料與無效資料合併
                    final_df = pd.concat([valid_df, invalid_df], ignore_index=True)
                    logger.info(f"已按照 order_sn 遞增順序排序（有效資料 {len(valid_df)} 筆）")
                else:
                    logger.warning("沒有有效的 order_sn 資料可排序")
                    
            except Exception as e:
                logger.warning(f"排序失敗，保持原始順序：{e}")
        else:
            logger.warning("找不到 order_sn 欄位，無法排序")
        
        return final_df
        
    except Exception as e:
        logger.error(f"合併所有檔案時發生錯誤：{e}")
        import traceback
        logger.error(f"錯誤詳情：{traceback.format_exc()}")
        return None

def save_merged_file(df: pd.DataFrame, output_dir: Path, logger: logging.Logger) -> bool:
    """儲存合併後的檔案"""
    try:
        # 生成輸出檔案名稱
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_filename = f"yahoo_orders_merged_{timestamp}.csv"
        output_path = output_dir / output_filename
        
        # 儲存檔案
        df.to_csv(output_path, index=False, encoding='utf-8-sig')
        logger.info(f"合併檔案已儲存至：{output_path}")
        
        return True
        
    except Exception as e:
        logger.error(f"儲存合併檔案時發生錯誤：{e}")
        return False

def main():
    """主函數"""
    logger = setup_logging()
    logger.info("=== Yahoo 訂單檔案合併作業開始 ===")
    
    # 檢查原始目錄
    if not RAW_DIR.exists():
        logger.error(f"原始目錄不存在：{RAW_DIR}")
        return 1
    
    # 載入欄位映射配置
    try:
        mapping = load_yahoo_fields_mapping()
        required_fields = get_required_fields(mapping)
        logger.info(f"必填欄位：{', '.join(required_fields)}")
        
        # 創建中文到英文的欄位映射
        zh_to_en_mapping = create_zh_to_en_mapping(mapping)
        logger.info(f"創建中文欄位映射，共 {len(zh_to_en_mapping)} 個對應關係")
        
    except Exception as e:
        logger.error(f"載入配置失敗：{e}")
        return 1
    
    # 尋找所有 CSV 檔案
    csv_files = list(RAW_DIR.glob("*.csv"))
    if not csv_files:
        logger.warning(f"在 {RAW_DIR} 中找不到 CSV 檔案")
        return 0
    
    logger.info(f"找到 {len(csv_files)} 個 CSV 檔案")
    
    # 合併所有檔案
    logger.info("開始合併所有檔案...")
    merged_df = merge_all_files(csv_files, logger, zh_to_en_mapping)
    
    if merged_df is not None:
        # 儲存合併後的檔案
        if save_merged_file(merged_df, TEMP_DIR, logger):
            logger.info("✅ 所有檔案合併成功")
        else:
            logger.error("❌ 檔案儲存失敗")
            return 1
    else:
        logger.error("❌ 檔案合併失敗")
        return 1
    
    # 輸出結果摘要
    logger.info("=" * 50)
    logger.info("處理結果摘要")
    logger.info("=" * 50)
    logger.info(f"處理檔案：{len(csv_files)} 個")
    logger.info(f"合併後資料：{len(merged_df)} 筆")
    logger.info(f"輸出目錄：{TEMP_DIR}")
    logger.info("=" * 50)
    
    logger.info("✅ 所有檔案合併完成！")
    return 0

if __name__ == "__main__":
    exit(main())
