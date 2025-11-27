#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
info_combination.py

将 financial_data_validator.py 与 bilibili_crawler.py 生成的CSV结果合并，
去除五列完全相同的重复数据，输出统一的CSV文件。
"""

import argparse
from pathlib import Path
from typing import List

import pandas as pd

COLUMNS: List[str] = ["股票代码", "公司名称", "财报名称", "报告日期", "PDF链接"]
RAW_CODE_COLUMN = "证券代码_raw"
OUTPUT_COLUMNS = COLUMNS + [RAW_CODE_COLUMN]
DEFAULT_VALIDATOR = Path("financial_data_validator.csv")
DEFAULT_BILIBILI = Path("bilibili_crawler.csv")
DEFAULT_OUTPUT = Path("info_combination.xlsx")


def load_csv(path: Path) -> pd.DataFrame:
    """加载CSV，如果不存在则返回空DataFrame。"""
    if not path or not path.exists():
        print(f"未找到文件：{path}. 跳过该数据源。")
        return pd.DataFrame(columns=OUTPUT_COLUMNS)

    try:
        df = pd.read_csv(path, dtype=str, keep_default_na=False)
    except UnicodeDecodeError:
        df = pd.read_csv(path, dtype=str, keep_default_na=False, encoding="utf-8")

    # 仅保留需要的列；缺失列填空字符串
    for column in COLUMNS:
        if column not in df.columns:
            df[column] = ""

    df = df[COLUMNS].copy()
    df = df.fillna("")
    df[RAW_CODE_COLUMN] = df["股票代码"].apply(extract_raw_code)
    df["来源文件"] = path.name  # 辅助统计
    return df


def extract_raw_code(code: str) -> str:
    """提取6位数字证券代码。"""
    if not code:
        return ""
    digits = "".join(filter(str.isdigit, str(code)))
    return digits[-6:].zfill(6) if digits else ""


def combine_data(validator_path: Path, bilibili_path: Path, output_path: Path) -> None:
    """合并两份数据并写入CSV。"""
    df_validator = load_csv(validator_path)
    df_bilibili = load_csv(bilibili_path)

    combined = pd.concat([df_validator, df_bilibili], ignore_index=True)

    if combined.empty:
        print("两个数据源均为空，未生成输出文件。")
        return

    before = len(combined)
    deduped = combined.drop_duplicates(subset=COLUMNS, keep="first").reset_index(drop=True)

    # 依据证券代码_raw选择最新报告
    deduped["_parsed_date"] = pd.to_datetime(deduped["报告日期"], errors="coerce")
    deduped = deduped.sort_values(by=["证券代码_raw", "_parsed_date"], ascending=[True, False])
    deduped = deduped.drop_duplicates(subset=[RAW_CODE_COLUMN], keep="first")
    deduped = deduped.drop(columns=["_parsed_date"])

    after = len(deduped)

    deduped[OUTPUT_COLUMNS].to_excel(output_path, index=False)
    print(f"合并完成，生成 {output_path.name} (Excel)")
    print(f"  输入记录数: {before}")
    print(f"  去重后记录数: {after}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="合并 validator 与 bilibili 爬虫生成的CSV数据，去除五列完全重复的记录。"
    )
    parser.add_argument(
        "--validator",
        type=Path,
        default=DEFAULT_VALIDATOR,
        help=f"financial_data_validator.py 输出文件路径，默认 {DEFAULT_VALIDATOR}",
    )
    parser.add_argument(
        "--bilibili",
        type=Path,
        default=DEFAULT_BILIBILI,
        help=f"bilibili_crawler.py 输出文件路径，默认 {DEFAULT_BILIBILI}",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"合并后输出文件路径，默认 {DEFAULT_OUTPUT}",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    combine_data(args.validator, args.bilibili, args.output)


if __name__ == "__main__":
    main()

