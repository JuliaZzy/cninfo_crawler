'''
@Project ：PycharmProjects
@File    ：巨潮资讯半年报爬虫（修改版）
@IDE     ：PyCharm
@Author  ：lingxiaotian（原版）+ 修改版
@Date    ：2025年
@修改说明：改为爬取2025年半年报，支持多个交易所，格式化公司代码
'''

#首先引入第三方库
import requests
import re
import pandas as pd
import time
from datetime import datetime, timedelta

#定义一个访问接口的函数
def get_report(page_num, date, exchange_column="szse", plate=""):
    """
    获取财报公告数据
    
    Args:
        page_num: 页码
        date: 日期范围，格式如 "2025-07-01~2025-07-31"
        exchange_column: 交易所代码，可选值：
            - "sse": 上交所（包含沪市、沪主板）
            - "szse": 深交所（包含深市、深主板、创业板）
            - "bj": 北交所
            - "neeq": 新三板
            - "star": 科创板（独立板块）
        plate: 板块细分（可选，通常留空即可，因为column已经包含了所有子板块）
    """
    url = "http://www.cninfo.com.cn/new/hisAnnouncement/query"
    headers = {
        "Accept": "*/*",
        "Accept-Encoding": "gzip, deflate",
        "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Host": "www.cninfo.com.cn",
        "Origin": "http://www.cninfo.com.cn",
        "Proxy-Connection": "keep-alive",
        "Referer": "http://www.cninfo.com.cn/new/commonUrl/pageOfSearch?url=disclosure/list/search&checkedCategory=category_bndbg_szsh",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36 Edg/113.0.1774.42",
        "X-Requested-With": "XMLHttpRequest"
    }
    '''
    参数信息
     column: 交易所代码（sse/szse/bj/neeq/star）
     category: category_bndbg_szsh 表示半年报
     seDate：查询时间范围
    '''
    data = {
        "pageNum": page_num,
        "pageSize": 30,
        "column": exchange_column,  # 支持多个交易所
        "tabName": "fulltext",
        "plate": plate,  # 板块细分（通常留空，因为column已包含所有子板块）
        "searchkey": "",
        "secid": "",
        "category": "category_bndbg_szsh",  # 半年报
        "trade": "",
        "seDate": date,
        "sortName": "code",
        "sortType": "asc",
        "isHLtitle": "false"
    }
    response = requests.post(url, data=data, headers=headers)
    return response


def downlaod_report(date_str, exchange_column="szse", exchange_name="深交所", plate=""):
    """
    下载指定日期的报告（参考financial_data_validator.py：按天查询更精确）
    
    Args:
        date_str: 日期字符串，格式如 "2025-07-01"
        exchange_column: 交易所代码
        exchange_name: 交易所名称（用于显示）
        plate: 板块细分
    """
    all_results = []
    page_num = 1
    
    # 按天查询：日期范围格式为 "2025-07-01~2025-07-01"
    date_range = f"{date_str}~{date_str}"
    
    try:
        response_test = get_report(page_num, date_range, exchange_column, plate)
        data_test = response_test.json()
        total_pages = data_test.get("totalpages", 0)
        if total_pages == 0:
            return all_results  # 无数据时静默返回，不打印
    except Exception as e:
        return all_results  # 失败时静默返回
    
    max_retries = 3 #最大重试次数
    retry_count = 0 #当前重试次数
    while page_num <= total_pages:
        response = None

        # 重试机制
        while retry_count <= max_retries:
            # 发送请求
            try:
                response = get_report(page_num, date_range, exchange_column, plate)
                response.raise_for_status()
                break
            except requests.exceptions.RequestException as e:
                print(f"出现错误！: {e}")
                print(f"5秒后重试...")
                time.sleep(5)
                retry_count += 1

        if retry_count > max_retries:
            print(f"{max_retries} 次重试后均失败. 跳过第 {page_num}页.")
            page_num += 1
            retry_count = 0
            continue
        else:
            # 解析数据
            try:
                data = response.json()
                # 尝试解析公告数据，如果解析失败则重试
                retry_count = 0
                while True:
                    try:
                        if data["announcements"] is None:
                            raise Exception("公告数据为空")
                        else:
                            all_results.extend(data["announcements"])
                        break
                    except (TypeError, KeyError) as e:
                        print(f"解析公告数据失败: {e}")
                        print(f"5秒后重试...")
                        time.sleep(5)
                        retry_count += 1
                        if retry_count > max_retries:
                            raise Exception("达到最大重试次数，跳过此页")
                        continue
                page_num += 1
            except (ValueError, KeyError) as e:
                print(f"解析响应数据失败: {e}")
                print(f"5秒后重试...")
                time.sleep(5)
                retry_count += 1
                if retry_count > max_retries:
                    raise Exception("达到最大重试次数，跳过此页")
                continue
    return all_results


def format_company_code(sec_code, exchange_column):
    """
    格式化公司代码：补齐6位并添加交易所后缀
    
    Args:
        sec_code: 原始证券代码
        exchange_column: 交易所代码
    
    Returns:
        格式化后的代码，如 "000001.SZ"
    """
    if not sec_code or sec_code == '未知代码':
        return sec_code
    
    sec_code_str = str(sec_code)
    
    # 如果是数字，补齐前导零到6位
    if sec_code_str.isdigit():
        sec_code_str = sec_code_str.zfill(6)
    
    # 根据交易所代码添加后缀
    if exchange_column == "sse" or sec_code_str.startswith('60') or sec_code_str.startswith('68'):
        return sec_code_str + '.SH'  # 上交所
    elif exchange_column == "szse" or sec_code_str.startswith('00') or sec_code_str.startswith('30'):
        return sec_code_str + '.SZ'  # 深交所
    elif exchange_column == "bj" or sec_code_str.startswith('83') or sec_code_str.startswith('87') or sec_code_str.startswith('92'):
        return sec_code_str + '.BJ'  # 北交所
    else:
        return sec_code_str  # 保持原样


def main(target_year=2025):
    """
    主函数：爬取2025年半年报数据
    
    Args:
        target_year: 目标年份，默认2025
    """
    global sum
    all_results = []
    
    # 定义要爬取的交易所列表
    # 注意：根据巨潮资讯网的分类
    # - szse（深交所）包含：深市、深主板、创业板
    # - sse（上交所）包含：沪市、沪主板
    # - star（科创板）是上交所的独立板块，需要单独爬取
    # - bj（北交所）是独立交易所
    # - neeq（新三板）是独立市场
    # 
    # 如果发现数据不全，可以尝试：
    # 1. 检查plate参数是否需要设置特定值
    # 2. 添加其他可能的交易所代码
    exchanges = [
        {"name": "上交所", "column": "sse", "plate": ""},      # 沪市、沪主板
        {"name": "深交所", "column": "szse", "plate": ""},     # 深市、深主板、创业板
        {"name": "科创板", "column": "star", "plate": ""},     # 科创板（独立板块）
        {"name": "北交所", "column": "bj", "plate": ""},       # 北交所
        {"name": "新三板", "column": "neeq", "plate": ""}      # 新三板
    ]
    
    # 如果需要更细分的板块，可以取消下面的注释并添加：
    # exchanges = [
    #     {"name": "上交所", "column": "sse", "plate": ""},
    #     {"name": "深交所", "column": "szse", "plate": ""},
    #     {"name": "深主板", "column": "szse", "plate": "szmb"},  # 深主板
    #     {"name": "创业板", "column": "szse", "plate": "cyb"},    # 创业板
    #     {"name": "科创板", "column": "star", "plate": ""},
    #     {"name": "北交所", "column": "bj", "plate": ""},
    #     {"name": "新三板", "column": "neeq", "plate": ""}
    # ]
    
    # 2025年半年报通常在7-9月发布
    # 参考financial_data_validator.py：按天分段更精确，避免遗漏数据
    start_date = datetime(target_year, 7, 1)
    end_date = datetime(target_year, 9, 30)
    
    # 按天生成日期列表（更精确，避免遗漏）
    date_list = [(start_date + timedelta(days=i)).strftime("%Y-%m-%d") 
                 for i in range((end_date - start_date).days + 1)]
    
    print(f"\n开始爬取{target_year}年半年报数据")
    print(f"时间范围: {start_date.strftime('%Y-%m-%d')} 到 {end_date.strftime('%Y-%m-%d')}")
    print(f"共 {len(date_list)} 天")
    print(f"交易所: {', '.join([ex['name'] for ex in exchanges])}")
    print(f"\n覆盖板块说明：")
    print(f"  - 深交所(szse): 包含 深市、深主板、创业板")
    print(f"  - 上交所(sse): 包含 沪市、沪主板")
    print(f"  - 科创板(star): 独立板块")
    print(f"  - 北交所(bj): 独立交易所")
    print(f"  - 新三板(neeq): 独立市场")
    print(f"\n注意：按天分段爬取，确保数据完整性\n")
    
    # 用于去重的集合，使用多个字段组合作为唯一标识
    seen_announcements = set()
    
    # 遍历每个交易所
    for exchange in exchanges:
        print(f"\n{'='*50}")
        print(f"开始爬取 {exchange['name']} ({exchange['column']})")
        print(f"{'='*50}")
        
        exchange_count = 0
        
        # 遍历每个日期（按天查询，更精确）
        for date_str in date_list:
            results = downlaod_report(date_str, exchange["column"], exchange["name"], exchange.get("plate", ""))
            
            if results:
                exchange_count += len(results)
            
            # 去重处理：只添加未出现过的公告（参考financial_data_validator.py的去重机制）
            for ann in results:
                # 使用多个字段组合作为唯一标识
                unique_key = (
                    ann.get('secCode', ''),
                    ann.get('announcementTitle', ''),
                    ann.get('announcementTime', ''),
                    ann.get('adjunctUrl', '')
                )
                
                if unique_key not in seen_announcements:
                    seen_announcements.add(unique_key)
                    all_results.append(ann)
            
            time.sleep(0.3)  # 避免请求过于频繁（参考financial_data_validator.py）
        
        print(f"  {exchange['name']} 共获取 {exchange_count} 条公告（去重前）")


    if not all_results:
        print("\n未获取到任何数据！")
        return
    
    print(f"\n共获取到 {len(all_results)} 条公告数据（已去重），正在生成CSV...")
    
    # 解析搜索结果并准备数据
    # 再次去重，使用处理后的数据作为唯一标识
    seen_csv_rows = set()
    processed_data = []
    
    for item in all_results:
        # 获取原始数据
        raw_code = item.get("secCode", "")
        company_name = item.get("secName", "未知公司")
        title = item.get("announcementTitle", "未知报告").strip()
        adjunct_url = item.get("adjunctUrl", "")
        announcement_time = item.get("announcementTime", "")
        
        # 处理时间戳
        if isinstance(announcement_time, int):
            date_str = datetime.fromtimestamp(announcement_time / 1000).strftime('%Y-%m-%d')
        elif isinstance(announcement_time, str):
            date_str = announcement_time.split(' ')[0]
        else:
            date_str = ""
        
        # 只保留2025年的报告
        if not date_str.startswith('2025'):
            continue
        
        # 过滤：排除包含"摘要"的报告
        if '摘要' in title:
            continue
        
        # 清理标题
        title = re.sub(r"<.*?>", "", title)  # 移除HTML标签
        title = title.strip()
        
        # 格式化公司代码 - 需要根据交易所判断
        # 先尝试从代码判断交易所
        exchange_column = "szse"  # 默认
        if raw_code:
            code_str = str(raw_code).zfill(6) if str(raw_code).isdigit() else str(raw_code)
            if code_str.startswith('60') or code_str.startswith('68'):
                exchange_column = "sse"
            elif code_str.startswith('83') or code_str.startswith('87') or code_str.startswith('92'):
                exchange_column = "bj"
        
        company_code = format_company_code(raw_code, exchange_column)
        
        # 生成PDF链接
        if adjunct_url:
            announcement_url = f"https://static.cninfo.com.cn/{adjunct_url}"
        else:
            announcement_url = ""
        
        # 检查标题是否包含排除关键词
        exclude_flag = False
        for keyword in exclude_keywords:
            if keyword in title:
                exclude_flag = True
                break

        # 如果标题不包含排除关键词，则将搜索结果添加到数据列表中
        if not exclude_flag:
            # 使用处理后的数据作为唯一标识，再次去重
            csv_unique_key = (company_code, company_name, title, date_str, announcement_url)
            
            if csv_unique_key not in seen_csv_rows:
                seen_csv_rows.add(csv_unique_key)
                processed_data.append({
                    "股票代码": company_code,
                    "公司名称": company_name,
                    "财报名称": title,
                    "报告日期": date_str,
                    "PDF链接": announcement_url
                })
    
    # 保存CSV文件
    if processed_data:
        df = pd.DataFrame(processed_data)
        output_filename = f"bilibili_crawler.csv"
        df.to_csv(output_filename, index=False, encoding='utf-8-sig')
        print(f"\nCSV文件已保存: {output_filename}")
        print(f"共处理 {len(processed_data)} 条有效数据")
    else:
        print("\n未找到有效数据，不生成CSV文件")


if __name__ == '__main__':
    # 全局变量
    # 排除列表：可以加入'更正后','修订版'来规避数据重复
    exclude_keywords = ['英文', '摘要', '已取消', '公告']
    
    # 设置目标年份
    target_year = 2025  # 爬取2025年半年报
    
    print("=" * 60)
    print("巨潮资讯半年报爬虫（修改版）")
    print("=" * 60)
    print(f"目标年份: {target_year}年")
    print(f"报告类型: 半年报")
    print(f"支持交易所: 上交所、深交所、北交所、新三板、科创板")
    print("=" * 60)
    
    try:
        main(target_year)
        print(f"\n{target_year}年半年报数据爬取完成！")
    except KeyboardInterrupt:
        print("\n\n程序被用户中断")
    except Exception as e:
        print(f"\n程序执行出错: {e}")
        import traceback
        traceback.print_exc()