# scripts/etmall_orders_etl/05_etmall_orders_product_matcher.py
"""
東森購物訂單資料產品匹配腳本

功能：
- 讀取由 04_etmall_orders_enricher.py 產生的增強後 CSV 檔案
- 載入 config/products.yaml 和 config/etmall_fields_mapping.json
- 使用 etmall_fields_mapping.json 進行中英文欄位名稱映射比對
- 以 '廠商商品號碼' (seller_product_sn) 欄位為鍵，與產品主檔的 'barcode' 進行匹配
- 新增 category, subcategory, brand, series, pet_type, product_name, item_code, sku,
  tags, spec, unit, origin, cost, supplier_code, supplier 等欄位
- 將最終的增強資料輸出到 temp/etmall/

使用方式：
直接執行此腳本
"""

import pandas as pd
from pathlib import Path
import sys
import logging
from datetime import datetime
import yaml
from typing import Dict, Any, List, Optional

def setup_logging(project_root: Path) -> None:
    """
    設定日誌功能，將日誌輸出到檔案和控制台
    並在每次執行前清除舊的日誌檔案
    """
    log_dir = project_root / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    # 清除舊的日誌檔案
    for log_file in log_dir.glob('etmall_orders_product_matcher_*.log'):
        try:
            log_file.unlink()
        except OSError as e:
            print(f"錯誤: 無法刪除舊日誌檔案 {log_file} - {e}")
            
    log_filename = f'etmall_orders_product_matcher_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log'
    log_path = log_dir / log_filename

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_path, 'w', 'utf-8'),
            logging.StreamHandler(sys.stdout)
        ]
    )

def load_yaml_config(file_path: Path) -> Dict[str, Any]:
    """
    載入 YAML 配置檔案
    
    Args:
        file_path: YAML 檔案路徑
        
    Returns:
        解析後的字典，失敗時返回空字典
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            data = yaml.safe_load(f)
            if data is None:
                logging.warning(f"YAML 檔案 {file_path} 為空或無效")
                return {}
            return data
    except FileNotFoundError:
        logging.error(f"錯誤：找不到配置檔案 {file_path}")
        return {}
    except yaml.YAMLError as e:
        logging.error(f"錯誤：解析 YAML 檔案失敗 {file_path} - {e}")
        return {}
    except Exception as e:
        logging.exception(f"錯誤：載入配置檔案時發生未知錯誤 {file_path}")
        return {}

def detect_field_name(df: pd.DataFrame, possible_names: List[str], fields_mapping: Dict[str, Any] = None) -> Optional[str]:
    """
    動態檢測欄位名稱，支援中文和英文欄位名稱
    使用 etmall_fields_mapping.json 進行中英文欄位名稱映射比對
    
    Args:
        df: DataFrame
        possible_names: 可能的欄位名稱列表
        fields_mapping: 欄位映射配置字典
        
    Returns:
        找到的欄位名稱，如果都沒找到則返回 None
    """
    # 首先檢查直接匹配
    for name in possible_names:
        if name in df.columns:
            return name
    
    # 如果沒有直接匹配，使用 fields_mapping 進行映射比對
    if fields_mapping:
        for name in possible_names:
            # 在 fields_mapping 中尋找對應的欄位配置
            for field_key, field_config in fields_mapping.items():
                zh_name = field_config.get('zh_name', '')
                if zh_name == name or field_key == name:
                    # 檢查映射的中文名稱是否在 DataFrame 中
                    if zh_name in df.columns:
                        logging.info(f"通過映射找到欄位：'{name}' -> '{zh_name}'")
                        return zh_name
                    # 檢查映射的英文名稱是否在 DataFrame 中
                    elif field_key in df.columns:
                        logging.info(f"通過映射找到欄位：'{name}' -> '{field_key}'")
                        return field_key
    
    return None

def create_products_dataframe(product_master_dict: Dict[str, Any]) -> pd.DataFrame:
    """
    將產品主檔字典轉換為 DataFrame
    
    Args:
        product_master_dict: 產品主檔字典 (key 就是 barcode)
        
    Returns:
        產品 DataFrame
    """
    if not product_master_dict:
        logging.error("產品主檔字典為空")
        return pd.DataFrame()
    
    try:
        # 將字典轉換為 DataFrame
        df_products = pd.DataFrame.from_dict(product_master_dict, orient='index')
        df_products.reset_index(inplace=True)
        
        # 檢查 YAML 中是否已經有 barcode 欄位
        if 'barcode' in df_products.columns:
            # 如果已經有 barcode 欄位，就使用 YAML 中的，丟棄從 index 創建的
            df_products.drop(columns=['index'], inplace=True)
            logging.info("使用 YAML 中現有的 barcode 欄位，忽略從 key 創建的欄位")
        else:
            # 如果沒有 barcode 欄位，將 index (key) 重新命名為 barcode
            df_products.rename(columns={'index': 'barcode'}, inplace=True)
            logging.info("將 YAML key 轉換為 barcode 欄位")
        
        logging.info(f"成功載入 {len(df_products)} 筆產品資料")
        
        # 確保沒有重複的欄位名稱
        columns = df_products.columns.tolist()
        unique_columns = list(dict.fromkeys(columns))  # 保持順序並去重
        if len(columns) != len(unique_columns):
            logging.warning(f"發現重複的欄位名稱，原欄位數：{len(columns)}，去重後：{len(unique_columns)}")
            df_products = df_products.loc[:, ~df_products.columns.duplicated()]
        
        # 顯示實際可用欄位
        available_columns = list(df_products.columns)
        logging.info(f"產品資料中的欄位：{available_columns}")
        
        # 檢查必要欄位
        required_columns = ['category', 'subcategory', 'brand', 'series', 'pet_type', 
                          'product_name', 'item_code', 'sku', 'tags', 'spec', 'unit', 
                          'origin', 'cost', 'supplier_code', 'supplier']
        
        missing_columns = [col for col in required_columns if col not in df_products.columns]
        if missing_columns:
            logging.warning(f"產品主檔中缺少以下欄位：{missing_columns}")
            # 為缺少的欄位添加空值
            for col in missing_columns:
                df_products[col] = ''
        
        # 確保 barcode 欄位為字串類型
        if 'barcode' in df_products.columns:
            df_products['barcode'] = df_products['barcode'].astype(str)
        
        return df_products
        
    except Exception as e:
        logging.exception(f"錯誤：轉換產品主檔為 DataFrame 時發生錯誤")
        return pd.DataFrame()

def remove_duplicate_barcodes(df_products: pd.DataFrame) -> pd.DataFrame:
    """
    移除重複的 barcode 記錄
    
    Args:
        df_products: 產品 DataFrame
        
    Returns:
        去重後的產品 DataFrame
    """
    if df_products.empty:
        return df_products
    
    if 'barcode' not in df_products.columns:
        logging.error("產品 DataFrame 中沒有 'barcode' 欄位")
        return df_products
    
    # 檢查是否有重複的 barcode
    original_count = len(df_products)
    duplicated_mask = df_products.duplicated(subset=['barcode'], keep=False)
    
    if duplicated_mask.any():
        duplicates = df_products[duplicated_mask].sort_values('barcode')
        logging.warning(f"在產品主檔中發現 {duplicated_mask.sum()} 筆重複的 barcode")
        
        # 顯示重複的條碼資訊（限制顯示數量避免日誌過長）
        if len(duplicates) <= 20:
            duplicate_info = duplicates[['barcode', 'product_name']].to_string()
            logging.warning(f"重複的條碼資訊：\n{duplicate_info}")
        else:
            sample_duplicates = duplicates[['barcode', 'product_name']].head(10).to_string()
            logging.warning(f"重複的條碼資訊（前10筆）：\n{sample_duplicates}")
        
        # 移除重複項目，保留第一筆
        df_products_dedup = df_products.drop_duplicates(subset=['barcode'], keep='first')
        dropped_count = original_count - len(df_products_dedup)
        logging.warning(f"總共移除了 {dropped_count} 筆重複的產品條碼資料")
        
        return df_products_dedup
    
    logging.info("產品主檔中沒有重複的 barcode")
    return df_products

def find_latest_enriched_file(input_dir: Path) -> Optional[Path]:
    """
    尋找最新的增強後檔案
    
    Args:
        input_dir: 輸入目錄
        
    Returns:
        最新檔案的路徑，未找到時返回 None
    """
    enriched_files = sorted(
        list(input_dir.glob('04_etmall_orders_enriched_*.csv')), 
        reverse=True
    )
    
    if not enriched_files:
        logging.error(f'在 {input_dir} 目錄下沒有找到任何增強後的檔案')
        return None
    
    latest_file = enriched_files[0]
    logging.info(f'找到最新檔案：{latest_file.name}')
    return latest_file

def load_orders_data(file_path: Path, fields_mapping: Dict[str, Any] = None) -> pd.DataFrame:
    """
    載入訂單資料
    
    Args:
        file_path: 訂單檔案路徑
        fields_mapping: 欄位映射配置字典
        
    Returns:
        訂單 DataFrame
    """
    try:
        df_orders = pd.read_csv(file_path, dtype=str)
        logging.info(f'已讀取訂單資料，總共 {len(df_orders)} 筆')
        
        # 動態檢測廠商商品號碼欄位名稱，使用欄位映射配置
        seller_product_sn_field = detect_field_name(df_orders, ['廠商商品號碼', 'seller_product_sn'], fields_mapping)
        
        if seller_product_sn_field is None:
            logging.error("訂單資料中沒有找到 '廠商商品號碼' 或 'seller_product_sn' 欄位，無法進行產品匹配")
            return pd.DataFrame()
        
        logging.info(f"使用欄位 '{seller_product_sn_field}' 作為產品匹配的 key")
        
        # 確保廠商商品號碼欄位為字串類型並處理空值
        df_orders[seller_product_sn_field] = df_orders[seller_product_sn_field].astype(str)
        df_orders[seller_product_sn_field] = df_orders[seller_product_sn_field].replace('nan', '')
        
        # 統計有效的廠商商品號碼
        valid_sns = df_orders[df_orders[seller_product_sn_field] != ''][seller_product_sn_field].nunique()
        logging.info(f'有效的廠商商品號碼數量：{valid_sns}')
        
        return df_orders
        
    except Exception as e:
        logging.exception(f'錯誤：讀取訂單檔案失敗：{file_path.name}')
        return pd.DataFrame()

def merge_orders_with_products(df_orders: pd.DataFrame, df_products: pd.DataFrame, fields_mapping: Dict[str, Any] = None) -> pd.DataFrame:
    """
    將訂單資料與產品主檔進行匹配
    
    Args:
        df_orders: 訂單 DataFrame
        df_products: 產品 DataFrame
        fields_mapping: 欄位映射配置字典
        
    Returns:
        匹配後的 DataFrame
    """
    if df_orders.empty or df_products.empty:
        logging.error("訂單資料或產品資料為空，無法進行匹配")
        return df_orders
    
    # 動態檢測廠商商品號碼欄位名稱，使用欄位映射配置
    seller_product_sn_field = detect_field_name(df_orders, ['廠商商品號碼', 'seller_product_sn'], fields_mapping)
    
    if seller_product_sn_field is None:
        logging.error("訂單資料中沒有找到 '廠商商品號碼' 或 'seller_product_sn' 欄位，無法進行產品匹配")
        return df_orders
    
    # 準備要合併的產品欄位 (不包含 barcode，因為它用於匹配)
    desired_product_columns = ['category', 'subcategory', 'brand', 'series', 'pet_type', 
                              'product_name', 'item_code', 'sku', 'tags', 'spec', 'unit', 
                              'origin', 'cost', 'supplier_code', 'supplier']
    
    # 檢查產品 DataFrame 中實際可用的欄位
    available_product_columns = [col for col in desired_product_columns if col in df_products.columns]
    missing_columns = [col for col in desired_product_columns if col not in df_products.columns]
    
    if missing_columns:
        logging.warning(f"產品資料中缺少以下欄位：{missing_columns}")
    
    # 要用於合併的欄位 (包含 barcode 用於匹配)
    merge_columns = ['barcode'] + available_product_columns
    
    logging.info(f"開始匹配訂單資料與產品主檔...")
    logging.info(f"訂單筆數：{len(df_orders)}")
    logging.info(f"產品筆數：{len(df_products)}")
    logging.info(f"將要合併的產品欄位：{available_product_columns}")
    logging.info(f"使用欄位 '{seller_product_sn_field}' 作為匹配 key")
    
    # 執行 left join，保留所有訂單資料
    try:
        df_matched = pd.merge(
            df_orders,
            df_products[merge_columns],
            left_on=seller_product_sn_field,
            right_on='barcode',
            how='left'
        )
        
        # 移除輔助的 barcode 欄位（如果它不在原始訂單資料中）
        if 'barcode' in df_matched.columns and 'barcode' not in df_orders.columns:
            df_matched.drop(columns=['barcode'], inplace=True)
            logging.info("已移除輔助的 barcode 欄位")
        elif 'barcode' in df_matched.columns:
            logging.info("保留 barcode 欄位（原訂單資料中已存在）")
        
        # 統計匹配結果 - 使用可用的欄位來判斷是否匹配成功
        matched_count = 0
        check_column = None
        
        # 按優先順序選擇一個欄位來檢查匹配狀態
        priority_columns = ['product_name', 'sku', 'brand', 'supplier']
        for col in priority_columns:
            if col in df_matched.columns:
                check_column = col
                break
        
        if check_column:
            matched_count = df_matched[df_matched[check_column].notna() & (df_matched[check_column] != '')].shape[0]
            unmatched_count = len(df_matched) - matched_count
            
            logging.info(f"匹配成功：{matched_count} 筆（基於 {check_column} 欄位判斷）")
            logging.info(f"匹配失敗：{unmatched_count} 筆")
            
            if unmatched_count > 0:
                # 顯示部分未匹配的廠商商品號碼
                unmatched_mask = df_matched[check_column].isna() | (df_matched[check_column] == '')
                unmatched_sns = df_matched[unmatched_mask][seller_product_sn_field].unique()
                sample_size = min(10, len(unmatched_sns))
                logging.warning(f"部分未匹配的廠商商品號碼（前{sample_size}筆）：{list(unmatched_sns[:sample_size])}")
        else:
            logging.warning("無法找到合適的欄位來統計匹配結果")
        
        # 填充空值
        df_matched = df_matched.fillna('')
        
        # 將產品相關的英文欄位名稱轉換為中文欄位名稱，以便 06 腳本進行映射
        if fields_mapping:
            logging.info("開始轉換產品欄位名稱為中文...")
            en_to_zh_mapping = {}
            for en_col, config in fields_mapping.items():
                zh_name = config.get('zh_name')
                if zh_name and en_col in df_matched.columns:
                    en_to_zh_mapping[en_col] = zh_name
            
            # 特殊處理 cost 欄位，直接重命名為 purchase_cost
            if 'cost' in df_matched.columns:
                en_to_zh_mapping['cost'] = 'purchase_cost'
                logging.info(f"特殊處理：將 'cost' 欄位重命名為 'purchase_cost'")
            
            # 重命名欄位
            if en_to_zh_mapping:
                df_matched = df_matched.rename(columns=en_to_zh_mapping)
                logging.info(f"已將以下欄位轉換為中文名稱：{list(en_to_zh_mapping.keys())}")
        
        logging.info("產品匹配完成")
        return df_matched
        
    except Exception as e:
        logging.exception("錯誤：合併訂單與產品資料時發生錯誤")
        return df_orders

def clean_old_files(output_dir: Path, pattern: str) -> None:
    """
    清除舊檔案
    
    Args:
        output_dir: 輸出目錄
        pattern: 檔案模式
    """
    try:
        old_files = list(output_dir.glob(pattern))
        for old_file in old_files:
            try:
                old_file.unlink()
                logging.info(f"已刪除舊檔案：{old_file.name}")
            except OSError as e:
                logging.error(f"錯誤：無法刪除舊檔案 {old_file.name} - {e}")
    except Exception as e:
        logging.exception(f"錯誤：清除舊檔案時發生錯誤")

def save_matched_data(df_matched: pd.DataFrame, output_dir: Path) -> Path:
    """
    儲存匹配後的資料
    
    Args:
        df_matched: 匹配後的 DataFrame
        output_dir: 輸出目錄
        
    Returns:
        輸出檔案路徑
    """
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_filename = f'05_etmall_orders_product_matched_{timestamp}.csv'
    output_path = output_dir / output_filename
    
    try:
        df_matched.to_csv(output_path, index=False, encoding='utf-8-sig')
        logging.info(f"已儲存匹配後資料：{output_path}")
        return output_path
    except Exception as e:
        logging.exception(f"錯誤：儲存匹配後資料時發生錯誤")
        raise

def main():
    """主要執行函數"""
    # 取得專案根目錄
    script_dir = Path(__file__).parent
    project_root = script_dir.parents[1]
    
    # 設定日誌
    setup_logging(project_root)
    logging.info("=== 東森購物訂單產品匹配開始 ===")

    # 設定路徑
    input_dir = project_root / 'temp' / 'etmall'
    output_dir = project_root / 'temp' / 'etmall'
    product_master_file = project_root / 'config' / 'products.yaml'
    fields_mapping_file = project_root / 'config' / 'etmall_fields_mapping.json'

    # 檢查目錄和檔案
    if not input_dir.exists() or not input_dir.is_dir():
        logging.error(f'錯誤：找不到輸入目錄 {input_dir}')
        sys.exit(1)
        
    if not output_dir.exists():
        output_dir.mkdir(parents=True, exist_ok=True)
        logging.info(f"已建立輸出目錄：{output_dir}")
        
    if not product_master_file.exists():
        logging.error(f'錯誤：找不到產品主檔 {product_master_file}')
        sys.exit(1)
        
    if not fields_mapping_file.exists():
        logging.error(f'錯誤：找不到欄位映射檔案 {fields_mapping_file}')
        sys.exit(1)

    # 清除舊的匹配檔案
    logging.info("清除舊的匹配檔案...")
    clean_old_files(output_dir, '05_etmall_orders_product_matched_*.csv')

    logging.info(f'讀取輸入目錄：{input_dir}')
    logging.info(f'輸出目錄：{output_dir}')
    logging.info(f'產品主檔：{product_master_file}')
    logging.info(f'欄位映射檔案：{fields_mapping_file}')

    # 載入欄位映射配置
    logging.info("\n=== 載入欄位映射配置 ===")
    fields_mapping = load_yaml_config(fields_mapping_file)
    if not fields_mapping:
        logging.error('無法載入欄位映射配置，停止執行')
        sys.exit(1)
    logging.info(f"已載入欄位映射配置，包含 {len(fields_mapping)} 個欄位定義")

    # 載入產品主檔
    logging.info("\n=== 載入產品主檔 ===")
    product_master_dict = load_yaml_config(product_master_file)
    if not product_master_dict:
        logging.error('無法載入產品主檔，停止執行')
        sys.exit(1)
    
    # 轉換為 DataFrame
    df_products = create_products_dataframe(product_master_dict)
    if df_products.empty:
        logging.error('無法建立產品 DataFrame，停止執行')
        sys.exit(1)
    
    # 移除重複的 barcode
    df_products = remove_duplicate_barcodes(df_products)

    # 尋找最新的訂單檔案
    logging.info("\n=== 載入訂單資料 ===")
    latest_enriched_file = find_latest_enriched_file(input_dir)
    if latest_enriched_file is None:
        sys.exit(1)

    # 載入訂單資料，傳入欄位映射配置
    df_orders = load_orders_data(latest_enriched_file, fields_mapping)
    if df_orders.empty:
        logging.error('無法載入訂單資料，停止執行')
        sys.exit(1)

    # 執行產品匹配，傳入欄位映射配置
    logging.info("\n=== 執行產品匹配 ===")
    df_matched = merge_orders_with_products(df_orders, df_products, fields_mapping)

    # 儲存結果
    logging.info("\n=== 儲存結果 ===")
    try:
        output_path = save_matched_data(df_matched, output_dir)
        
        logging.info(f"\n=== 產品匹配完成 ===")
        logging.info(f"最終匹配檔案：{output_path}")
        logging.info(f"最終資料筆數：{len(df_matched)}")
        logging.info(f"資料欄位數：{len(df_matched.columns)}")
        
    except Exception as e:
        logging.error("儲存結果時發生錯誤，停止執行")
        sys.exit(1)

if __name__ == '__main__':
    main()