#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试查看基金排行数据的完整结构
"""

import sys
import os

# 添加项目根目录到PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.scrapers.eastmoney import EastMoneyScraper
from app.scrapers.base import DataSource


def test_fund_data_structure():
    """测试查看基金排行数据的完整结构"""
    print("开始测试基金排行数据结构...")
    
    # 创建爬虫实例
    scraper = EastMoneyScraper()
    
    # 获取单页数据
    page_data = scraper._get_fund_rank_page(page=1, page_size=1)
    
    if not page_data or not page_data.get("data"):
        print("获取数据失败")
        return
    
    fund_list = page_data.get("data", [])
    if not fund_list:
        print("基金列表为空")
        return
    
    # 查看第一条基金数据
    first_fund = fund_list[0]
    print(f"\n基金数据结构：")
    for key, value in first_fund.items():
        print(f"{key}: {value} (类型: {type(value).__name__})")
    
    # 直接查看原始数据字符串，了解完整字段
    print("\n获取原始数据字符串...")
    import requests
    import time
    import re
    
    # 构建请求URL
    rank_api_url = "https://fund.eastmoney.com/data/rankhandler.aspx"
    params = {
        "op": "ph",
        "dt": "kf",
        "ft": "all",
        "rs": "",
        "gs": 0,
        "sc": "dm",
        "st": "asc",
        "sd": "2024-12-25",
        "ed": "2025-12-25",
        "qdii": "",
        "tabSubtype": ",,,,,",
        "pi": 1,
        "pn": 1,
        "dx": 1,
        "v": str(time.time()),
    }
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Referer": f"https://fund.eastmoney.com/data/fundranking.html",
    }
    
    response = requests.get(rank_api_url, params=params, headers=headers, timeout=10)
    response.raise_for_status()
    content = response.text
    
    # 提取原始数据字符串
    rank_data_match = re.search(r"var rankData = (\{.*?\});", content, re.DOTALL)
    if rank_data_match:
        rank_data_str = rank_data_match.group(1)
        print(f"原始数据结构: {rank_data_str[:2000]}...")
        
        # 提取datas字段
        datas_pattern = r"datas:\[(.*?)\]"
        datas_match = re.search(datas_pattern, rank_data_str, re.DOTALL)
        if datas_match:
            datas_content = datas_match.group(1)
            fund_strings = re.findall(r'"([^"]+)"', datas_content)
            if fund_strings:
                first_fund_str = fund_strings[0]
                print(f"\n第一个基金的完整原始数据字符串:")
                print(first_fund_str)
                
                # 查看字段数量
                fields = first_fund_str.split(",")
                print(f"\n字段总数: {len(fields)}")
                print("各个字段的值:")
                for i, field in enumerate(fields):
                    print(f"字段{i}: {field}")
    
    print("\n测试完成！")


if __name__ == "__main__":
    test_fund_data_structure()