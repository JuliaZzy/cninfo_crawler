#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
巨潮信息网财报数据爬虫
功能：
1. 爬取巨潮信息网指定日期范围内的财报数据
2. 输出包含股票代码、公司名称、财报名称、报告日期、PDF链接的CSV文件
"""

import argparse
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import partial
import time
from datetime import datetime, timedelta
import warnings
from pathlib import Path

# 抑制警告信息
warnings.filterwarnings("ignore")

# 报告类型映射
REPORT_TYPE_MAP = {
    "yjdbg": {"category": "category_yjdbg_szsh", "label": "一季度"},
    "bndbg": {"category": "category_bndbg_szsh", "label": "半年报"},
    "sjdbg": {"category": "category_sjdbg_szsh", "label": "三季度"},
    "ndbg": {"category": "category_ndbg_szsh", "label": "年报"},
}


def parse_args():
    parser = argparse.ArgumentParser(description="财务数据验证器 - 支持日期范围和报告类型参数")
    parser.add_argument(
        "--start-date",
        type=str,
        required=True,
        help="开始日期，格式：YYYY-MM-DD，例如：2025-08-01",
    )
    parser.add_argument(
        "--end-date",
        type=str,
        required=True,
        help="结束日期，格式：YYYY-MM-DD，例如：2025-08-31",
    )
    parser.add_argument(
        "--report-type",
        type=str,
        required=True,
        choices=REPORT_TYPE_MAP.keys(),
        help="报告类型：yjdbg=一季度, bndbg=半年报, sjdbg=三季度, ndbg=年报",
    )
    return parser.parse_args()


def validate_pdf_access(announcement_info, session, headers, target_years):
    """
    快速验证PDF是否可以访问（只检查响应头，不下载完整文件）
    
    Args:
        announcement_info (dict): 公告信息
        session (requests.Session): 请求会话
        headers (dict): 请求头
        target_years (list): 目标年份列表，用于检查财报标题是否包含这些年份
    
    Returns:
        dict or None: 如果PDF可访问，返回包含股票代码、公司名称、财报名称、报告日期、PDF链接的字典；否则返回None
    """
    file_url = 'https://static.cninfo.com.cn/' + announcement_info['adjunctUrl']
    sec_name = announcement_info.get('secName', '未知公司')
    raw_sec_code = announcement_info.get('secCode', '未知代码')
    sec_code = raw_sec_code
    announcement_title = announcement_info.get('announcementTitle', '未知报告')
    announcement_time = announcement_info.get('announcementTime', '')
    
    # 处理报告日期
    if isinstance(announcement_time, int):
        date_str = datetime.fromtimestamp(announcement_time / 1000).strftime('%Y-%m-%d')
    elif isinstance(announcement_time, str):
        date_str = announcement_time.split(' ')[0] if announcement_time else ''
    else:
        date_str = ''
    
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
        elif (
            sec_code_str.startswith('83')
            or sec_code_str.startswith('87')
            or sec_code_str.startswith('92')
            or sec_code_str.startswith('43')
        ):
            sec_code = sec_code_str + '.BJ'  # 北交所
        else:
            sec_code = sec_code_str  # 保持原样
    
    # 过滤：标题含目标年份或标题不含任何数字
    # 检查标题是否包含数字
    has_digit = bool(re.search(r'\d', announcement_title))
    # 检查标题是否包含目标年份中的任何年份
    if target_years:
        year_found = any(str(year) in announcement_title for year in target_years)
    else:
        year_found = False
    
    # 新逻辑：标题含目标年份或标题不含任何数字
    if not (year_found or not has_digit):
        return None
    
    # 过滤：排除包含摘要/英文版的报告
    if ('摘要' in announcement_title) or ('英文版' in announcement_title):
        return None
    
    try:
        # 使用HEAD请求快速检查文件是否存在（更快）
        # 如果HEAD不支持，则使用GET但只读取少量字节
        try:
            response = session.head(file_url, headers=headers, timeout=10, allow_redirects=True)
            if response.status_code == 200:
                content_type = response.headers.get('Content-Type', '')
                if 'application/pdf' in content_type:
                    return {
                        "股票代码": sec_code,
                        "公司名称": sec_name,
                        "财报名称": announcement_title,
                        "报告日期": date_str,
                        "PDF链接": file_url
                    }
        except:
            # 如果HEAD失败，尝试GET但只读取前1024字节验证
            response = session.get(file_url, headers=headers, timeout=10, stream=True)
            response.raise_for_status()
            
            # 只读取前1024字节来验证是否为PDF
            chunk = next(response.iter_content(1024), b'')
            if chunk.startswith(b'%PDF'):
                return {
                    "股票代码": sec_code,
                    "公司名称": sec_name,
                    "财报名称": announcement_title,
                    "报告日期": date_str,
                    "PDF链接": file_url
                }
        
        return None
        
    except requests.exceptions.RequestException:
        return None
    except Exception as e:
        return None


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
        for report_category in report_categories:
            page_num = 1
            total_pages = None  # 总页数，第一次请求后获取
            empty_pages_count = 0  # 连续空页计数
            max_empty_pages = 3  # 连续3页无有效数据就停止
            
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
                    
                    # 显示进度（每页都显示，使用flush确保实时输出）
                    if total_pages:
                        print(f"  正在查询第 {page_num}/{total_pages} 页...", end='', flush=True)
                    else:
                        print(f"  正在查询第 {page_num} 页...", end='', flush=True)
                    
                    response = session.post(api_url, headers=headers, data=post_data, timeout=20)
                    response.raise_for_status()
                    
                    # 检查响应状态
                    if response.status_code != 200:
                        print(f"\n  API返回状态码: {response.status_code}")
                        break
                    
                    try:
                        data = response.json()
                    except ValueError:
                        print(f"\n  API返回的不是JSON格式数据")
                        break
                    
                    # 获取总页数（第一次请求时）
                    if total_pages is None and isinstance(data, dict):
                        total_pages = data.get('totalpages', 0)
                        announcements_in_response = data.get('announcements', [])

                        # 检查是否有数据：totalpages > 0 或者 announcements 不为空
                        if total_pages > 0 or announcements_in_response:
                            if total_pages > 0:
                                print(f"\n  共 {total_pages} 页数据")
                            else:
                                # totalpages为0但announcements有数据的情况
                                print(f"\n  API返回数据 {len(announcements_in_response)} 条（totalpages=0）")
                        else:
                            print(f"\n  无数据")
                            break
                    
                    # 检查是否超过总页数（更严格的停止条件）
                    if total_pages and total_pages > 0:  # 只有当total_pages > 0时才检查超限
                        if page_num > total_pages + 3:
                            # 超过总页数3页后强制停止
                            print(f"\n  已超过总页数 {total_pages} 页，强制停止（当前第 {page_num} 页）")
                            break
                        elif page_num > total_pages and empty_pages_count >= 2:
                            # 超过总页数且连续2页无有效数据，立即停止
                            print(f"\n  已超过总页数 {total_pages} 且连续 {empty_pages_count} 页无有效数据，停止查询")
                            break
                    elif total_pages == 0 and page_num > 1:
                        # total_pages为0时，只处理第1页，之后停止
                        print(f"\n  total_pages为0，只处理第1页，停止查询")
                        break
                    
                    # 添加最大页数限制，防止无限循环（安全措施）
                    max_pages_limit = 500
                    if page_num > max_pages_limit:
                        print(f"\n  达到最大页数限制 {max_pages_limit}，强制停止")
                        break

                    # 当total_pages为0时，只处理第1页
                    if total_pages == 0 and page_num > 1:
                        print(f"\n  total_pages为0，已处理完第1页，停止查询")
                        break
                    
                    # 处理不同的响应格式
                    if isinstance(data, list):
                        announcements = data
                    elif isinstance(data, dict):
                        announcements = data.get('announcements', [])
                    else:
                        announcements = []
                    
                    # 如果API返回空数据，说明已经到最后一页
                    if not announcements:
                        if page_num == 1:
                            print(f"\n  无数据")
                        else:
                            print(f"\n  查询完成（共 {page_num-1} 页）")
                        break
                    
                    # 显示当前页获取到的原始数据量
                    raw_count = len(announcements)
                    
                    # 去重处理
                    filtered_announcements = []
                    for ann in announcements:
                        # 使用多个字段组合作为唯一标识
                        unique_key = (
                            ann.get('secCode', ''),
                            ann.get('announcementTitle', ''),
                            ann.get('announcementTime', ''),
                            ann.get('adjunctUrl', '')
                        )
                        
                        if unique_key not in seen_announcements:
                            seen_announcements.add(unique_key)
                            filtered_announcements.append(ann)
                    
                    # 将过滤后的数据添加到总列表
                    all_announcements.extend(filtered_announcements)
                    
                    # 显示当前页获取到的数据量（过滤后）
                    if len(filtered_announcements) < raw_count:
                        print(f" - 本页 {raw_count} 条（过滤后 {len(filtered_announcements)} 条）", end='\n')
                    else:
                        print(f" - 本页 {len(filtered_announcements)} 条", end='\n')
                    
                    # 检查过滤后是否有有效数据
                    if len(filtered_announcements) == 0:
                        empty_pages_count += 1
                        # 如果连续多页都没有有效数据，就停止（不管是否超过总页数）
                        if empty_pages_count >= max_empty_pages:
                            if total_pages and page_num > total_pages:
                                print(f"\n  连续 {empty_pages_count} 页无有效数据，且已超过总页数 {total_pages}，停止查询")
                            else:
                                print(f"\n  连续 {empty_pages_count} 页无有效数据，停止查询")
                            break
                    else:
                        empty_pages_count = 0  # 重置计数
                    
                    page_num += 1
                    time.sleep(0.3)  # 避免请求过于频繁
                    
                except requests.exceptions.HTTPError as e:
                    if e.response.status_code == 404:
                        print(f"  API接口返回404，可能接口已失效: {api_url}")
                    else:
                        print(f"  API请求HTTP错误: {e.response.status_code}")
                    break
                except requests.exceptions.RequestException as e:
                    print(f"  API请求失败: {e}")
                    break
                except Exception as e:
                    print(f"  API请求异常: {e}")
                    break
    
    return all_announcements


def main(start_date_str: str, end_date_str: str, report_type: str):
    """
    主函数 - 快速验证财报PDF可访问性
    
    Args:
        start_date_str: 开始日期字符串，格式：YYYY-MM-DD
        end_date_str: 结束日期字符串，格式：YYYY-MM-DD
        report_type: 报告类型，可选值：yjdbg, bndbg, sjdbg, ndbg
    """
    # 验证报告类型
    if report_type not in REPORT_TYPE_MAP:
        raise ValueError(f"不支持的报告类型：{report_type}，可选值：{list(REPORT_TYPE_MAP.keys())}")
    
    # 解析日期
    try:
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    except ValueError as e:
        raise ValueError(f"日期格式错误，请使用 YYYY-MM-DD 格式：{e}")
    
    if start_date > end_date:
        raise ValueError(f"开始日期 {start_date_str} 不能晚于结束日期 {end_date_str}")
    
    # 获取报告类别和标签
    report_config = REPORT_TYPE_MAP[report_type]
    report_category = report_config["category"]
    report_label = report_config["label"]
    
    # 生成日期列表
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
                 for i in range((end_date - start_date).days + 1)]

    # 提取日期范围内的所有年份（用于年份检查）
    # 逻辑：将日期整体后移3个月后取年份
    # 例如：2025-04-01到2026-03-31之间，减去3个月后是2025-01-01到2025-12-31，检查2025
    # 例如：2025-07-01到2025-09-30之间，减去3个月后是2025-04-01到2025-06-30，检查2025
    
    def subtract_3_months(date):
        """将日期减去3个月"""
        month = date.month - 3
        year = date.year
        if month <= 0:
            month += 12
            year -= 1
        return datetime(year, month, date.day)
    
    # 将开始和结束日期都减去3个月
    start_date_shifted = subtract_3_months(start_date)
    end_date_shifted = subtract_3_months(end_date)
    
    # 提取减去3个月后的年份范围
    target_years = list(set([start_date_shifted.year, end_date_shifted.year]))
    if end_date_shifted.year > start_date_shifted.year:
        target_years = list(range(start_date_shifted.year, end_date_shifted.year + 1))

    # 生成动态输出文件名：listed_companies_{start_date}_{end_date}_{report_type}_{timestamp}.csv
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    OUTPUT_FILENAME = Path(f"listed_companies_{start_date_str.replace('-', '')}_{end_date_str.replace('-', '')}_{report_type}_{timestamp}.csv")

    print("=" * 60)
    print("财务数据验证器 - 快速验证财报PDF可访问性")
    print("=" * 60)
    
    print(f"时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    print(f"报告类型: {report_label}")
    print("=" * 60)
    
    session = requests.Session()
    api_url = 'http://www.cninfo.com.cn/new/hisAnnouncement/query'

    # 请求头配置
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl?url=disclosure/list/notice",
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # 报告类别
    report_categories = [report_category]
    
    # 使用有效的API接口
    api_urls = [
        'http://www.cninfo.com.cn/new/hisAnnouncement/query'  # 主要API接口
    ]
    
    # 交易所列表
    exchanges = [
        {"name": "上交所", "column": "sse"},
        {"name": "深交所", "column": "szse"},
        {"name": "北交所", "column": "bj"},
        {"name": "新三板", "column": "neeq"},
        {"name": "科创板", "column": "star"}
    ]

    all_valid_reports = []
    start_time = time.time()
    
    # 统计信息
    total_announcements = 0
    total_valid = 0
    
    # 遍历每个交易所
    for exchange in exchanges:
        print(f"\n{'='*50}")
        print(f"开始验证 {exchange['name']} ({exchange['column']})")
        print(f"{'='*50}")
        
        exchange_announcements = 0
        exchange_valid = 0
        
        # 遍历每个日期
        for idx, date_str in enumerate(date_list, 1):
            # 显示进度
            if len(date_list) > 1:
                print(f"  处理 {idx}/{len(date_list)}: {date_str}")
            else:
                print(f"  查询日期: {date_str}")
            
            # 使用多API接口获取公告数据
            announcements = get_announcements_multi_api(
                session, headers, exchange, date_str, report_categories, api_urls
            )
            
            if not announcements:
                print(f"  未获取到公告数据\n")
                continue
            
            exchange_announcements += len(announcements)
            print(f"  获取到 {len(announcements)} 个公告，正在验证PDF可访问性...")
            
            # 使用线程池并发验证（提高速度）
            with ThreadPoolExecutor(max_workers=10) as executor:
                # 使用 partial 传递 target_years 参数
                validate_func = partial(validate_pdf_access, session=session, headers=headers, target_years=target_years)
                future_to_info = {
                    executor.submit(validate_func, ann): ann 
                    for ann in announcements
                }
                for future in as_completed(future_to_info):
                    try:
                        result = future.result()
                        if result:
                            all_valid_reports.append(result)
                            exchange_valid += 1
                    except Exception as exc:
                        pass
            
            if len(announcements) > 0:
                print(f"  验证完成，{exchange_valid} 个PDF可访问")
        
        # 交易所统计
        print(f"\n{exchange['name']} 统计:")
        print(f"  总公告数: {exchange_announcements}")
        print(f"  可访问PDF: {exchange_valid}")
        
        total_announcements += exchange_announcements
        total_valid += exchange_valid
    
    # 显示总体统计信息
    print(f"\n验证结果统计:")
    print(f"  总公告数: {total_announcements}")
    print(f"  可访问PDF: {total_valid}")
    print(f"  可访问率: {(total_valid/total_announcements*100):.1f}%" if total_announcements > 0 else "  可访问率: 0%")
    
    # 生成CSV报告
    print("\n===== 正在生成CSV报告... =====")
    if all_valid_reports:
        df = pd.DataFrame(all_valid_reports)
        
        # 去掉摘要/英文版
        original_count = len(df)
        df = df[~df['财报名称'].str.contains('摘要|英文版', na=False)].copy()
        after_filter = len(df)
        print(f"已剔除摘要/英文版，共移除 {original_count - after_filter} 条记录")

        print("正在进行数据去重...")
        df = df.drop_duplicates(subset=['股票代码', '公司名称', '财报名称'], keep='first')

        # 按日期保留每个代码最新的一条
        df['_parsed_date'] = pd.to_datetime(df['报告日期'], errors='coerce')
        df = df.sort_values(by=['股票代码', '_parsed_date'], ascending=[True, False])
        df = df.drop_duplicates(subset=['股票代码'], keep='first')
        df = df.drop(columns=['_parsed_date'])
        final_count = len(df)
        print(f"最终保留 {final_count} 条（每个股票代码保留最新一条）")
        
        # 确保列顺序：股票代码、公司名称、财报名称、报告日期、PDF链接
        df = df[['股票代码', '公司名称', '财报名称', '报告日期', 'PDF链接']]
        
        df.to_csv(OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
        print(f"CSV报告生成完毕！已保存为 ./{OUTPUT_FILENAME.name}")
        print(f"共 {len(df)} 条记录")
    else:
        print("未找到可访问的PDF，不生成CSV文件。")
        
    end_time = time.time()
    print(f"\n总耗时: {(end_time - start_time):.2f} 秒")
    print("程序执行完毕")


if __name__ == "__main__":
    args = parse_args()
    try:
        main(args.start_date, args.end_date, args.report_type)
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()
