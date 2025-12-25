#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试东方财富基金API，查看返回的数据结构
"""

import requests
import time
import re


def test_eastmoney_api():
    """测试东方财富基金API"""
    print("开始测试东方财富基金API...")
    
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
        "Referer": "https://fund.eastmoney.com/data/fundranking.html",
    }
    
    try:
        response = requests.get(rank_api_url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        content = response.text
        
        print("\nAPI返回内容:")
        print(content)
        
        # 提取datas字段
        datas_pattern = r"datas:\[(.*?)\]"
        datas_match = re.search(datas_pattern, content, re.DOTALL)
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
                    
    except Exception as e:
        print(f"请求失败: {e}")


if __name__ == "__main__":
    test_eastmoney_api()