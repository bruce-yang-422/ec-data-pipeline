# scripts/excel_password_remover/remover.py
"""
Excel 密碼移除工具

功能：
- 使用 msoffcrypto-tool 解開 Excel 開啟密碼
- 支援多種 Office 檔案格式
- 自動處理未加密檔案

輸入：加密的 Excel 檔案路徑和密碼
輸出：解密後的 Excel 檔案

Authors: 楊翔志 & AI Collective
Studio: tranquility-base
"""

import msoffcrypto
import shutil
from pathlib import Path
from typing import Union

def remove_password(input_path: Union[str, Path], output_path: Union[str, Path], password: str) -> None:
    """
    使用 msoffcrypto-tool 解開 Excel 開啟密碼，另存為 output_path。
    若檔案未加密或非 Office 格式，直接複製。
    """
    # 支援 Path 物件或 str
    input_path = str(input_path)
    output_path = str(output_path)

    with open(input_path, "rb") as f_in:
        office_file = msoffcrypto.OfficeFile(f_in)
        try:
            office_file.load_key(password=password)
            with open(output_path, "wb") as f_out:
                office_file.decrypt(f_out)
        except msoffcrypto.exceptions.FileFormatError as e:
            # 檔案未加密時有多種例外訊息，統一處理
            if "Unencrypted document" in str(e) or "File is not encrypted" in str(e):
                shutil.copyfile(input_path, output_path)
            else:
                raise
        except Exception as e:
            # 其它例外再報錯
            raise RuntimeError(f"密碼移除失敗，訊息: {e}")

