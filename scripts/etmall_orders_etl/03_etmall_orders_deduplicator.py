import sys
import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

import pandas as pd


def setup_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )


def get_latest_file(directory: Path, pattern: str) -> Optional[Path]:
    candidates = sorted(directory.glob(pattern), key=lambda p: p.stat().st_mtime, reverse=True)
    return candidates[0] if candidates else None


def load_mapping_config(mapping_path: Path) -> Dict[str, Dict[str, str]]:
    if not mapping_path.exists():
        logging.warning(f'找不到欄位映射檔案：{mapping_path}，將跳過最終欄位重排')
        return {}
    with mapping_path.open('r', encoding='utf-8') as f:
        return json.load(f)


def reorder_columns_by_mapping(df: pd.DataFrame, mapping_config: Dict[str, Dict[str, str]]) -> pd.DataFrame:
    if not mapping_config:
        return df
    ordered_columns = []
    existing_columns = set(df.columns)
    sorted_fields = sorted(mapping_config.items(), key=lambda x: int(x[1].get('order', '999')))
    for field_key, _cfg in sorted_fields:
        if field_key in existing_columns:
            ordered_columns.append(field_key)
            existing_columns.remove(field_key)
    remaining_columns = sorted(list(existing_columns))
    ordered_columns.extend(remaining_columns)
    return df[ordered_columns]


def normalize_key_fields(df: pd.DataFrame) -> pd.DataFrame:
    df_norm = df.copy()
    if 'order_sn' not in df_norm.columns:
        df_norm['order_sn'] = ''
    if 'item_no' not in df_norm.columns:
        df_norm['item_no'] = ''
    df_norm['order_sn'] = df_norm['order_sn'].astype(str).fillna('')
    df_norm['item_no'] = df_norm['item_no'].astype(str).fillna('')
    # 去除空白並標準化 item_no（移除小數點尾巴如 '1.0'）
    def _clean_item_no(val: str) -> str:
        s = str(val).strip()
        if s.endswith('.0'):
            s = s[:-2]
        return s
    df_norm['item_no'] = df_norm['item_no'].map(_clean_item_no)
    # 建立 order_line_uid
    df_norm['order_line_uid'] = df_norm['order_sn'].str.strip() + '_' + df_norm['item_no'].str.strip()
    return df_norm


def merge_group_prefer_sales_then_fill(group: pd.DataFrame) -> pd.Series:
    # 以銷售報表為優先，若無則取第一筆
    sales_rows = group[group['data_source'] == 'sales_report']
    base = sales_rows.iloc[-1].copy() if not sales_rows.empty else group.iloc[0].copy()
    # 用其餘資料補空值
    for _idx, row in group.iterrows():
        for col in base.index:
            if col in ('data_source',):
                continue
            if (pd.isna(base[col]) or base[col] == '') and (not pd.isna(row[col]) and row[col] != ''):
                base[col] = row[col]
    return base


def main() -> None:
    setup_logging()

    project_root = Path(__file__).resolve().parents[2]
    temp_dir = project_root / 'temp' / 'etmall'
    mapping_path = project_root / 'config' / 'etmall_fields_mapping.json'

    logging.info('尋找最新的中間檔...')
    latest_sales = get_latest_file(temp_dir, '02_etmall_orders_sales_report_*.csv')
    latest_detail = get_latest_file(temp_dir, '02_etmall_orders_detail_report_*.csv')

    if latest_sales is None and latest_detail is None:
        logging.error('找不到任何 02 中間檔，請先執行 02 腳本')
        sys.exit(1)

    if latest_sales:
        logging.info(f'最新銷售報表：{latest_sales.name}')
    else:
        logging.warning('未找到銷售報表中間檔，將僅以明細報表合併')

    if latest_detail:
        logging.info(f'最新明細報表：{latest_detail.name}')
    else:
        logging.warning('未找到明細報表中間檔，將僅以銷售報表合併')

    dfs = []
    if latest_sales:
        df_sales = pd.read_csv(latest_sales, encoding='utf-8-sig')
        df_sales['data_source'] = 'sales_report'
        dfs.append(df_sales)
    if latest_detail:
        df_detail = pd.read_csv(latest_detail, encoding='utf-8-sig')
        df_detail['data_source'] = 'detail_report'
        dfs.append(df_detail)

    combined = pd.concat(dfs, ignore_index=True, sort=False)
    logging.info(f'合併原始筆數：{len(combined)}')

    # 標準化 key 欄位並建立 order_line_uid
    combined = normalize_key_fields(combined)

    # 去重：以 order_sn + item_no 為 key（order_line_uid）
    if 'order_line_uid' not in combined.columns:
        logging.error('缺少 order_line_uid 欄位，無法去重')
        sys.exit(1)

    # 僅針對有效 key 進行分組合併
    valid_mask = (~combined['order_line_uid'].astype(str).eq('') & combined['order_line_uid'].notna())
    valid_df = combined[valid_mask]
    empty_df = combined[~valid_mask]

    if valid_df.empty:
        logging.warning('沒有有效的 order_line_uid，將輸出原始合併資料（無去重）')
        dedup_df = combined
    else:
        dedup_df = valid_df.groupby('order_line_uid', as_index=False, group_keys=False).apply(merge_group_prefer_sales_then_fill)  # type: ignore[assignment]
        if not empty_df.empty:
            dedup_df = pd.concat([dedup_df, empty_df], ignore_index=True, sort=False)

    logging.info(f'去重後筆數：{len(dedup_df)}（減少 {len(combined) - len(dedup_df)}）')

    # 欄位重排（若有 mapping）
    mapping_config = load_mapping_config(mapping_path)
    dedup_df = reorder_columns_by_mapping(dedup_df, mapping_config)

    # 補足 order_time（若存在相關欄位）
    if 'order_time' in dedup_df.columns:
        dedup_df['order_time'] = dedup_df['order_time'].astype(str).fillna('')

    # 排序與 NaN 處理
    if 'order_line_uid' in dedup_df.columns:
        dedup_df = dedup_df.sort_values('order_line_uid').reset_index(drop=True)
    dedup_df = dedup_df.fillna('')

    # 輸出
    ts = datetime.now().strftime('%Y%m%d_%H%M%S')
    out_path = temp_dir / f'03_etmall_orders_merged_{ts}.csv'
    dedup_df.to_csv(out_path, index=False, encoding='utf-8-sig')
    logging.info(f'已輸出合併去重檔案：{out_path}')


if __name__ == '__main__':
    main()


