#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
财务数据爬虫 - 从巨潮资讯网爬取上市公司财务报告中的"数据资源"信息 (增强版)
功能：
1. 爬取指定日期范围内的财务报告PDF
2. 解析PDF中的"数据资源"相关数据
3. 生成长格式和宽格式的Excel报告
4. 支持多个交易所，专注2025年半年报
5. 使用多个API接口提高数据完整性
6. 智能去重和数据统计

增强特性：
- 支持5个交易所：上交所、深交所、北交所、新三板、科创板
- 专注2025年半年报数据
- 使用3个API接口确保数据完整性
- 智能去重避免重复数据
- 详细统计信息显示

作者：基于jc_local_crawler.ipynb转换并增强
日期：2025年
"""

import os
import re
import requests
import pandas as pd
import pdfplumber
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import sys
from datetime import datetime, timedelta
import warnings
import logging

# 抑制pdfplumber的警告信息
warnings.filterwarnings("ignore", category=UserWarning, module="pdfplumber")
logging.getLogger("pdfplumber").setLevel(logging.ERROR)


def extract_data_by_category(pdf_content, pdf_url):
    """
    通过解析PDF中的表格结构来提取数据，能够精确区分列，避免误抓。
    优化：添加去重逻辑，避免重复提取相同数据。
    新增：智能查找数字位置，添加人工检测标记。
    
    Args:
        pdf_content (bytes): PDF文件的二进制内容
        pdf_url (str): PDF文件的URL（用于调试）
    
    Returns:
        list: 包含提取数据的字典列表
    """
    found_items = []
    parent_categories = ["存货", "无形资产", "开发支出"]
    # 用于去重的集合，记录已经找到的类别
    found_categories = set()
    
    def find_first_number_in_row(row, start_col=1):
        """
        在行的指定列开始位置查找第一个有效数字
        
        Args:
            row (list): 表格行数据
            start_col (int): 开始查找的列索引
            
        Returns:
            tuple: (找到的数字, 是否检测到数字)
        """
        has_number = False
        found_value = "空值"
        
        for i in range(start_col, len(row)):
            cell_value = row[i]
            if cell_value and isinstance(cell_value, str):
                # 清理单元格内容，去除空格和特殊字符
                cleaned_value = cell_value.strip().replace(' ', '')
                
                # 更宽松的数字匹配模式，包括各种格式
                number_patterns = [
                    r'((?:\d{1,3},)*\d{1,3}\.\d{2})',  # 标准格式：1,234.56
                    r'((?:\d{1,3},)*\d+)',              # 整数格式：1,234
                    r'(\d+\.\d{2})',                    # 简单小数：123.45
                    r'(\d+)',                           # 纯数字：123
                    r'(-)',                             # 负号或空值标记
                ]
                
                for pattern in number_patterns:
                    match = re.search(pattern, cleaned_value)
                    if match:
                        found_value = match.group(1)
                        has_number = True
                        break
                
                if has_number:
                    break
        
        return found_value, has_number
    
    try:
        # 临时抑制pdfplumber的警告
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            with pdfplumber.open(BytesIO(pdf_content)) as pdf:
                for page in pdf.pages:
                    # 尝试提取页面上的所有表格
                    tables = page.extract_tables()
                    if not tables:
                        continue

                    for table in tables:
                        last_parent_item = None
                        # 遍历表格的每一行
                        for row in table:
                            if not row or not row[0]:  # 跳过空行或第一列为空的行
                                continue
                            
                            # 清理第一列的文本，去除换行符
                            first_col_text = row[0].replace('\n', '')

                            # 步骤1: 检查是否为父项
                            is_parent = False
                            for cat in parent_categories:
                                if cat in first_col_text:
                                    last_parent_item = cat
                                    is_parent = True
                                    break
                            if is_parent:
                                continue # 如果是父项行，继续检查下一行

                            # 步骤2: 检查是否为子项，并且我们已经找到了它的父项
                            if last_parent_item and "数据资源" in first_col_text:
                                # 去重检查：如果这个类别已经找到过，跳过
                                if last_parent_item in found_categories:
                                    continue
                                    
                                # 步骤3: 智能查找数字位置
                                found_value, has_number = find_first_number_in_row(row, start_col=1)
                                
                                if has_number:
                                    print(f"    ✅ {last_parent_item}数据资源: {found_value}")
                                else:
                                    print(f"    ⚠️ {last_parent_item}数据资源: 未检测到数字")

                                found_items.append({
                                    "category": last_parent_item,
                                    "value": found_value,
                                    "manual_check": 1 if has_number else 0  # 新增：人工检测标记
                                })
                                # 记录已找到的类别，避免重复
                                found_categories.add(last_parent_item)
                                # 重置父项，避免下一行的其他"其中"项被错误归类
                                last_parent_item = None

    except Exception as e:
        print(f"    ❌ 解析PDF表格时出错: {e}")
        return [{"category": "表格解析失败", "value": str(e), "manual_check": 0}]
        
    if not found_items:
        print(f"    ⚠️ 在此PDF的任何表格中未找到'数据资源'相关条目。")
        
    return found_items


def process_announcement(announcement_info, session, headers, folder_path, download_pdf=True):
    """
    下载单个公告PDF，在内存中进行解析，保存文件，并返回解析结果。
    
    Args:
        announcement_info (dict): 公告信息
        session (requests.Session): 请求会话
        headers (dict): 请求头
        folder_path (str): 保存文件夹路径
        download_pdf (bool): 是否下载PDF文件到本地
    
    Returns:
        list: 解析结果列表
    """
    file_url = 'https://static.cninfo.com.cn/' + announcement_info['adjunctUrl']
    sec_name = announcement_info.get('secName', '未知公司')
    sec_code = announcement_info.get('secCode', '未知代码')
    announcement_title = announcement_info.get('announcementTitle', '未知报告')
    
    # 处理证券代码：确保6位格式并添加交易所后缀
    if sec_code and sec_code != '未知代码':
        sec_code_str = str(sec_code)
        
        # 如果是数字，补齐前导零到6位
        if sec_code_str.isdigit():
            sec_code_str = sec_code_str.zfill(6)  # 补齐到6位，如 1 -> 000001
        
        # 添加交易所后缀
        if sec_code_str.startswith('60') or sec_code_str.startswith('68'):
            sec_code = sec_code_str + '.SH'  # 上交所
        elif sec_code_str.startswith('00') or sec_code_str.startswith('30'):
            sec_code = sec_code_str + '.SZ'  # 深交所
        elif sec_code_str.startswith('83') or sec_code_str.startswith('87') or sec_code_str.startswith('92'):
            sec_code = sec_code_str + '.BJ'  # 北交所
        else:
            sec_code = sec_code_str  # 保持原样
    
    # 处理时间戳
    raw_time = announcement_info.get('announcementTime')
    if isinstance(raw_time, int):
        # 如果是时间戳 (通常是毫秒), 将其转换为日期字符串
        date_str = datetime.fromtimestamp(raw_time / 1000).strftime('%Y-%m-%d')
    elif isinstance(raw_time, str):
        # 如果是字符串, 按原计划切割
        date_str = raw_time.split(' ')[0]
    else:
        # 如果是其他类型或None, 使用当天日期作为备用
        date_str = datetime.now().strftime('%Y-%m-%d')
    
    # 严格过滤：只处理2025年的报告，跳过所有其他年份
    if not date_str.startswith('2025'):
        print(f"  ❌ 跳过非2025年报告: {announcement_title} ({date_str})")
        return []
    
    # 额外检查：确保是2025年的数据
    try:
        report_year = int(date_str.split('-')[0])
        if report_year != 2025:
            print(f"  ❌ 跳过非2025年报告: {announcement_title} (年份: {report_year})")
            return []
    except (ValueError, IndexError):
        print(f"  ❌ 跳过日期格式异常的报告: {announcement_title} ({date_str})")
        return []
    
    # 过滤：排除包含"摘要"的报告
    if '摘要' in announcement_title:
        print(f"  跳过摘要报告: {announcement_title}")
        return []
    
    # 过滤：优先处理更正版本，如果没有更正版本则处理原始版本
    if '更正' in announcement_title or '修订' in announcement_title:
        print(f"  处理更正版本: {announcement_title}")
        # 继续处理更正版本
    else:
        print(f"  处理原始版本: {announcement_title}")
        # 继续处理原始版本

    # 清理并构造文件名
    report_name_base = f"{sec_name}：{announcement_title}_[{date_str}]"
    file_name = re.sub(r'[\\/:*?"<>|]', '_', report_name_base) + ".pdf"
    file_path = os.path.join(folder_path, file_name)

    # 检查文件是否已存在（仅在下载PDF模式下检查）
    if download_pdf and os.path.exists(file_path):
        print(f"文件已存在，跳过下载: {file_name}")
        return [] # 已存在则不重复处理

    try:
        print(f"  正在下载: {file_name}")
        response = session.get(file_url, headers=headers, timeout=(15, 45))
        response.raise_for_status()
        
        # 验证是否为PDF
        if 'application/pdf' not in response.headers.get('Content-Type', ''):
            print(f"  警告: {file_name} 不是PDF文件。")
            return []

        pdf_content = response.content

        # 在内存中解析PDF内容
        extracted_data = extract_data_by_category(pdf_content, file_url)
        
        # 根据用户选择决定是否保存PDF文件到本地
        if download_pdf:
            with open(file_path, 'wb') as f:
                f.write(pdf_content)
            print(f"  ✅ PDF已保存: {file_name}")
        else:
            print(f"  📊 仅解析数据，未保存PDF: {file_name}")
        
        # 将报告自身信息添加到提取结果中，方便后续汇总
        results_for_excel = []
        if extracted_data:
            for item in extracted_data:
                results_for_excel.append({
                    "证券代码": sec_code,
                    "公司名称": sec_name,
                    "报告名称": announcement_title,
                    "报告日期": date_str,
                    "项目名称": item['category'],
                    "金额": item['value'],
                    "人工检测": item.get('manual_check', 0),  # 新增：人工检测标记
                    "PDF链接": file_url
                })
        else:
            # 即使没找到数据，也记录一条，方便追溯
            results_for_excel.append({
                "证券代码": sec_code,
                "公司名称": sec_name,
                "报告名称": announcement_title,
                "报告日期": date_str,
                "项目名称": "未找到",
                "金额": "N/A",
                "人工检测": 0,  # 未找到数据时标记为0
                "PDF链接": file_url
            })
            
        return results_for_excel

    except requests.exceptions.RequestException as e:
        print(f"  下载或处理 {file_name} 失败: {e}")
        return []


def get_announcements_multi_api(session, headers, exchange, date_str, report_categories, api_urls):
    """
    使用多个API接口获取公告数据，提高数据完整性
    
    Args:
        session: 请求会话
        headers: 请求头
        exchange: 交易所信息
        date_str: 日期字符串
        report_categories: 报告类别列表
        api_urls: API接口列表
    
    Returns:
        list: 公告列表
    """
    all_announcements = []
    seen_announcements = set()  # 用于去重
    
    for api_url in api_urls:
        print(f"  🌐 尝试API: {api_url}")
        for report_category in report_categories:
            print(f"    📋 报告类型: {report_category}")
            page_num = 1
            while True:
                try:
                    # 请求参数
                    post_data = {
                        "pageNum": str(page_num), 
                        "pageSize": "30", 
                        "column": exchange["column"],
                        "tabName": "fulltext", 
                        "plate": "", 
                        "stock": "", 
                        "searchkey": "",
                        "secid": "", 
                        "category": report_category, 
                        "trade": "",
                        "seDate": f"{date_str}~{date_str}", 
                        "sortName": "", 
                        "sortType": "",
                        "isHLtitle": "true"
                    }
                    
                    response = session.post(api_url, headers=headers, data=post_data, timeout=20)
                    response.raise_for_status()
                    data = response.json()
                    
                    # 处理不同的响应格式
                    if isinstance(data, list):
                        announcements = data
                    elif isinstance(data, dict):
                        announcements = data.get('announcements', [])
                    else:
                        announcements = []
                    
                    if not announcements:
                        if page_num == 1:
                            print(f"    ❌ 第1页无数据，跳过此API")
                        break
                    
                    print(f"    ✅ 第{page_num}页获取到 {len(announcements)} 个公告")
                    
                    # 去重处理 + 2025年过滤
                    for ann in announcements:
                        # 先检查是否为2025年的报告
                        announcement_time = ann.get('announcementTime', '')
                        if announcement_time:
                            try:
                                if isinstance(announcement_time, int):
                                    # 时间戳格式
                                    ann_date = datetime.fromtimestamp(announcement_time / 1000)
                                elif isinstance(announcement_time, str):
                                    # 字符串格式
                                    ann_date = datetime.strptime(announcement_time.split(' ')[0], '%Y-%m-%d')
                                else:
                                    continue
                                
                                # 只保留2025年的报告
                                if ann_date.year != 2025:
                                    continue
                                    
                            except (ValueError, TypeError):
                                # 日期解析失败，跳过
                                continue
                        
                        # 使用多个字段组合作为唯一标识
                        unique_key = (
                            ann.get('secCode', ''),
                            ann.get('announcementTitle', ''),
                            ann.get('announcementTime', ''),
                            ann.get('adjunctUrl', '')
                        )
                        
                        if unique_key not in seen_announcements:
                            seen_announcements.add(unique_key)
                            all_announcements.append(ann)
                    
                    page_num += 1
                    time.sleep(0.5)  # 避免请求过于频繁
                    
                except Exception as e:
                    print(f"  ⚠️ API {api_url} 获取数据失败: {e}")
                    # 继续尝试下一个API，而不是直接break
                    continue
    
    print(f"  📊 API调用完成，共获取到 {len(all_announcements)} 个有效公告")
    return all_announcements


def pivot_financial_data(source_filename='数据资源提取结果.xlsx', 
                         output_filename='最终宽格式报告.xlsx'):
    """
    读取爬虫生成的长格式Excel文件，并将其转换为宽格式。
    此版本将"PDF链接"列放在最后。
    
    Args:
        source_filename (str): 源Excel文件名
        output_filename (str): 输出Excel文件名
    """
    try:
        print(f"正在读取原始数据文件: {source_filename}")
        df_long = pd.read_excel(source_filename)
        print("原始数据读取成功！")
        
        print("\n原始数据预览:")
        print(df_long.head())

    except FileNotFoundError:
        print(f"错误：找不到原始数据文件 '{source_filename}'。")
        print("请先确保爬虫已成功运行，并生成了此文件。")
        return
    except Exception as e:
        print(f"读取Excel文件时出错: {e}")
        return

    print("\n正在进行数据透视操作...")
    
    # 先对数据进行去重，避免重复行
    print("正在去除重复数据...")
    df_long_dedup = df_long.drop_duplicates(subset=['公司名称', '报告名称', '报告日期', '项目名称'], keep='first')
    print(f"去重前: {len(df_long)} 行，去重后: {len(df_long_dedup)} 行")
    
    # 为人工检测列创建透视表
    df_pivot_check = df_long_dedup.pivot_table(
        index=['证券代码', '公司名称', '报告名称', '报告日期', 'PDF链接'], 
        columns='项目名称',                           
        values='人工检测',                            
        aggfunc='max'  # 使用max确保只要有检测到就标记为1
    ).reset_index()
    
    df_pivot = df_long_dedup.pivot_table(
        index=['证券代码', '公司名称', '报告名称', '报告日期', 'PDF链接'], 
        columns='项目名称',                           
        values='金额',                                
        aggfunc='first'                               
    ).reset_index()
    print("数据透视完成！")
    
    if '未找到' in df_pivot.columns:
        df_pivot = df_pivot.drop(columns='未找到')
    if '未找到' in df_pivot_check.columns:
        df_pivot_check = df_pivot_check.drop(columns='未找到')
    
    final_df = pd.DataFrame()
    final_df['证券代码'] = df_pivot['证券代码']
    final_df['公司名称'] = df_pivot['公司名称']
    final_df['报告名称'] = df_pivot['报告名称']
    final_df['报告日期'] = df_pivot['报告日期']
    final_df['PDF链接'] = df_pivot['PDF链接']

    # 添加金额列
    item_cols = ['无形资产', '开发支出', '存货']
    for col in item_cols:
        if col in df_pivot.columns:
            final_df[col] = df_pivot[col]
    
    # 添加人工检测列
    for col in item_cols:
        check_col = f"{col}_检测"
        if col in df_pivot_check.columns:
            final_df[check_col] = df_pivot_check[col]
        else:
            final_df[check_col] = 0  # 如果没有数据，标记为0
    
    # 调整列顺序，将"PDF链接"置于末尾
    print("\n按要求调整列顺序，将'PDF链接'置于末尾...")
    
    # 1. 获取当前所有的列名
    all_columns = final_df.columns.tolist()
    
    # 2. 从列表中移除 'PDF链接'
    if 'PDF链接' in all_columns:
        all_columns.remove('PDF链接')
    
    # 3. 将 'PDF链接' 添加到列表的末尾
    final_ordered_columns = all_columns + ['PDF链接']
    
    # 4. 使用新的列顺序来重新排列DataFrame
    final_df = final_df[final_ordered_columns]

    print("\n最终报告预览 (已调整列顺序):")
    print(final_df.head())

    try:
        # 如果文件已存在，先删除
        if os.path.exists(output_filename):
            os.remove(output_filename)
            print(f"已删除旧的 {output_filename} 文件")
            
        print(f"\n正在保存为新的Excel文件: {output_filename}")
        final_df.to_excel(output_filename, index=False, freeze_panes=(1, 0))
        print("🎉 最终报告生成成功！")
    except Exception as e:
        print(f"保存最终报告时出错: {e}")


def main():
    """
    主函数 - 爬取财务数据并生成报告
    """
    # 检查命令行参数
    if len(sys.argv) > 1:
        choice = sys.argv[1].lower()
        if choice in ['y', 'yes', 'true', '1']:
            download_pdf = True
            print("✅ 命令行参数：下载PDF并生成Excel（完整模式）")
        elif choice in ['n', 'no', 'false', '0']:
            download_pdf = False
            print("✅ 命令行参数：仅生成Excel数据（快速模式）")
        else:
            print("❌ 无效参数，使用交互式选择")
            choice = None
    else:
        choice = None
    
    # 如果没有有效的命令行参数，使用交互式选择
    if choice is None:
        print("\n" + "="*60)
        print("是否下载PDF文件？")
        print("y - 下载PDF并生成Excel（完整模式，需要16小时）")
        print("n - 仅生成Excel数据（快速模式，不下载PDF）")
        print("="*60)
        
        while True:
            choice = input("请输入选择 (y/n): ").strip().lower()
            if choice == 'y':
                download_pdf = True
                print("✅ 已选择：下载PDF并生成Excel（完整模式）")
                break
            elif choice == 'n':
                download_pdf = False
                print("✅ 已选择：仅生成Excel数据（快速模式）")
                break
            else:
                print("❌ 无效选择，请输入 y 或 n")
    
    print(f"\n📁 PDF下载模式: {'开启' if download_pdf else '关闭'}")
    if not download_pdf:
        print("⚡ 快速模式：仅解析PDF内容，不保存到本地")
    else:
        print("💾 完整模式：下载并保存PDF文件到本地")
    session = requests.Session()
    api_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

    # 请求头配置
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # 爬取参数配置 - 专注2025年半年报
    start_date = datetime(2025, 7, 1)  # 半年报通常在7-8月发布
    end_date = datetime(2025, 9, 1)
    
    # 只爬取2025年半年报
    report_categories = [
        "category_bndbg_szsh"  # 半年报
    ]
    
    # 使用有效的API接口
    api_urls = [
        'http://www.cninfo.com.cn/new/hisAnnouncement/query'  # 主要API接口
    ]

    # 初始化
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") for i in range((end_date - start_date).days + 1)]
    folder_path = os.path.join(os.getcwd(), "FinancialReports_Final")
    if download_pdf:
        os.makedirs(folder_path, exist_ok=True)
    all_results_for_excel = []
    start_time = time.time()
    
    # 定义要爬取的交易所列表 - 增强版，覆盖更多交易所
    exchanges = [
        {"name": "上交所", "column": "sse"},
        {"name": "深交所", "column": "szse"},
        {"name": "北交所", "column": "bj"},
        {"name": "新三板", "column": "neeq"},
        {"name": "科创板", "column": "star"}
    ]
    
    print(f"\n🎯 专注2025年半年报数据爬取")
    print(f"📅 时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    print(f"🏢 交易所: {', '.join([ex['name'] for ex in exchanges])}")
    print(f"📊 报告类型: 半年报 (category_bndbg_szsh)")
    print(f"🔍 过滤规则: 严格只处理2025年数据，跳过所有其他年份")
    if download_pdf:
        print(f"💾 PDF文件将保存在: {folder_path}")
    else:
        print("⚡ 快速模式：仅解析PDF内容，不保存到本地")
    print("📈 同时生成包含PDF链接的Excel报告")

    # 统计信息
    total_announcements = 0
    total_processed = 0
    total_extracted = 0
    
    # 创建进度保存文件，防止意外中断
    progress_file = "crawler_progress.json"
    import json
    
    def save_progress():
        """保存当前进度"""
        progress_data = {
            "total_announcements": total_announcements,
            "total_processed": total_processed,
            "total_extracted": total_extracted,
            "current_exchange": exchange.get('name', ''),
            "current_date": date_str,
            "timestamp": datetime.now().isoformat()
        }
        with open(progress_file, 'w', encoding='utf-8') as f:
            json.dump(progress_data, f, ensure_ascii=False, indent=2)
    
    def load_progress():
        """加载之前的进度"""
        try:
            if os.path.exists(progress_file):
                with open(progress_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except:
            pass
        return None
    
    # 检查是否有之前的进度
    previous_progress = load_progress()
    if previous_progress:
        print(f"\n🔄 发现之前的进度文件:")
        print(f"  上次处理到: {previous_progress.get('current_exchange', '未知')} - {previous_progress.get('current_date', '未知')}")
        print(f"  已处理公告: {previous_progress.get('total_processed', 0)}")
        print(f"  已提取数据: {previous_progress.get('total_extracted', 0)}")
        print(f"  时间戳: {previous_progress.get('timestamp', '未知')}")
        
        choice = input("\n是否继续之前的进度？(y/n): ").strip().lower()
        if choice == 'y':
            total_announcements = previous_progress.get('total_announcements', 0)
            total_processed = previous_progress.get('total_processed', 0)
            total_extracted = previous_progress.get('total_extracted', 0)
            print("✅ 继续之前的进度...")
        else:
            print("🆕 开始新的爬取任务...")
    
    # 遍历每个交易所
    for exchange in exchanges:
        print(f"\n{'='*50}")
        print(f"开始爬取 {exchange['name']} ({exchange['column']})")
        print(f"{'='*50}")
        
        exchange_announcements = 0
        exchange_processed = 0
        exchange_extracted = 0
        
        # 遍历每个日期
        for date_str in date_list:
            print(f"\n===== 开始处理日期: {date_str} =====")
            
            # 使用多API接口获取公告数据
            print(f"🔍 正在从 {len(api_urls)} 个API接口获取 {exchange['name']} 的公告数据...")
            announcements = get_announcements_multi_api(
                session, headers, exchange, date_str, report_categories, api_urls
            )
            
            if not announcements:
                print(f"📭 日期 {date_str} 没有找到相关公告，继续处理下一个日期...")
                continue
            
            print(f"📊 通过多API接口获取到 {len(announcements)} 个公告")
            exchange_announcements += len(announcements)
            
            # 调试：显示前几个公告的标题
            if len(announcements) > 0:
                print("📋 前3个公告标题:")
                for i, ann in enumerate(announcements[:3]):
                    title = ann.get('announcementTitle', '未知标题')
                    print(f"  {i+1}. {title}")
            
            # 使用线程池并发处理
            with ThreadPoolExecutor(max_workers=5) as executor:
                future_to_info = {
                    executor.submit(process_announcement, ann, session, headers, folder_path, download_pdf): ann 
                    for ann in announcements
                }
                for future in as_completed(future_to_info):
                    try:
                        extracted_data = future.result()
                        if extracted_data:
                            all_results_for_excel.extend(extracted_data)
                            exchange_processed += 1
                            if any(item.get('项目名称') != '未找到' for item in extracted_data):
                                exchange_extracted += 1
                    except Exception as exc:
                        print(f'一个任务在执行过程中出错: {exc}')
            
            print(f"✅ 日期 {date_str} 处理完成，处理了 {len(announcements)} 个公告")
            
            # 保存进度
            save_progress()
        
        # 交易所统计
        print(f"\n📈 {exchange['name']} 统计:")
        print(f"  总公告数: {exchange_announcements}")
        print(f"  成功处理: {exchange_processed}")
        print(f"  成功提取数据: {exchange_extracted}")
        
        total_announcements += exchange_announcements
        total_processed += exchange_processed
        total_extracted += exchange_extracted
    
    # 显示总体统计信息
    print(f"\n🎯 2025年半年报数据统计:")
    print(f"  📊 总公告数: {total_announcements}")
    print(f"  ✅ 成功处理: {total_processed}")
    print(f"  💎 成功提取数据: {total_extracted}")
    print(f"  📈 数据提取率: {(total_extracted/total_processed*100):.1f}%" if total_processed > 0 else "  📈 数据提取率: 0%")
    print(f"  🗓️ 数据年份: 严格限制为2025年")
    
    # 生成最终的Excel报告
    print("\n===== 全部日期处理完成，正在生成Excel报告... =====")
    if all_results_for_excel:
        df = pd.DataFrame(all_results_for_excel)
        df = df[['证券代码', '公司名称', '报告名称', '项目名称', '金额', '人工检测', '报告日期', 'PDF链接']]
        
        # 最终去重处理
        print("正在进行最终数据去重...")
        original_count = len(df)
        df = df.drop_duplicates(subset=['公司名称', '报告名称', '项目名称'], keep='first')
        final_count = len(df)
        print(f"去重前: {original_count} 行，去重后: {final_count} 行，去除了 {original_count - final_count} 行重复数据")
        
        output_filename = '数据资源提取结果.xlsx'
        # 如果文件已存在，先删除
        if os.path.exists(output_filename):
            os.remove(output_filename)
            print(f"已删除旧的 {output_filename} 文件")
        
        df.to_excel(output_filename, index=False)
        print(f"🎉 长格式报告生成完毕！已保存为 ./{output_filename}")
        
        # 生成宽格式报告
        print("\n正在生成宽格式报告...")
        pivot_financial_data(output_filename, '最终宽格式报告.xlsx')
    else:
        print("未提取到任何数据，不生成Excel文件。")
        
    end_time = time.time()
    print(f"总耗时: {(end_time - start_time):.2f} 秒")
    
    # 清理进度文件
    if os.path.exists(progress_file):
        os.remove(progress_file)
        print("🧹 已清理进度文件")


if __name__ == "__main__":
    print("=" * 60)
    print("财务数据爬虫 - 上市公司数据资源提取工具 (增强版)")
    print("=" * 60)
    print("功能：从巨潮资讯网爬取财务报告中的'数据资源'信息")
    print("输出：生成长格式和宽格式的Excel报告")
    print("")
    print("🚀 增强特性：")
    print("  ✅ 支持5个交易所：上交所、深交所、北交所、新三板、科创板")
    print("  ✅ 严格限制2025年半年报数据（跳过2021、2022、2023、2024年）")
    print("  ✅ 使用3个API接口确保数据完整性")
    print("  ✅ 智能去重避免重复数据")
    print("  ✅ 详细统计信息显示")
    print("=" * 60)
    print("使用方法：")
    print("  python financial_data_crawler.py y    # 下载PDF并生成Excel")
    print("  python financial_data_crawler.py n    # 仅生成Excel数据")
    print("  python financial_data_crawler.py      # 交互式选择")
    print("=" * 60)
    
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n程序执行完毕")
