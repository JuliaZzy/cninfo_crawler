#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
run_pipeline.py

顺序执行：
1. bilibili_crawler.py
2. financial_data_validator.py
3. 在前两步成功后运行 info_combination.py
"""

import subprocess
import sys
from pathlib import Path


SCRIPT_BILIBILI = Path("bilibili_crawler.py")
SCRIPT_VALIDATOR = Path("financial_data_validator.py")
SCRIPT_COMBINE = Path("info_combination.py")


def run_script(script: Path, desc: str) -> None:
    if not script.exists():
        raise FileNotFoundError(f"未找到 {desc} 脚本：{script}")

    print(f"\n===== 开始执行 {desc}: {script} =====")
    result = subprocess.run([sys.executable, str(script)], text=True)

    if result.returncode != 0:
        raise RuntimeError(f"{desc} 执行失败（退出码 {result.returncode}）。")

    print(f"===== {desc} 执行完成 =====\n")


def main():
    run_script(SCRIPT_BILIBILI, "B站爬虫")
    run_script(SCRIPT_VALIDATOR, "财报验证器")
    run_script(SCRIPT_COMBINE, "数据合并清洗")
    print("🎉 全部任务执行完成！")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"\n流水线执行失败：{exc}")
        sys.exit(1)

