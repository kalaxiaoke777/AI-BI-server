#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试新功能：基金公司、可购买状态和历史涨幅
"""

import sys
import os
import time
import requests

# 添加项目根目录到PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# API基础URL
BASE_URL = "http://localhost:8000/api/v1"

def test_api_health():
    """测试API健康状态"""
    print("测试API健康状态...")
    url = "http://localhost:8000/health"
    response = requests.get(url)
    print(f"健康检查状态码: {response.status_code}")
    print(f"响应内容: {response.json()}")
    return response.status_code == 200

def test_import_funds():
    """测试导入基金列表"""
    print("\n测试导入基金列表...")
    url = f"{BASE_URL}/fund/import?source=eastmoney"
    response = requests.post(url)
    print(f"导入基金状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"响应内容: {response.json()}")
    return response.status_code == 200

def test_get_funds():
    """测试获取基金列表"""
    print("\n测试获取基金列表...")
    url = f"{BASE_URL}/fund/?page=1&page_size=5"
    response = requests.get(url)
    print(f"获取基金列表状态码: {response.status_code}")
    if response.status_code == 200:
        data = response.json()
        print(f"响应内容: {data}")
        return data.get("total", 0) > 0
    return False

def test_update_fund_growth():
    """测试更新基金历史涨幅数据"""
    print("\n测试更新基金历史涨幅数据...")
    # 只更新前5个基金，避免请求过多
    url = f"{BASE_URL}/fund/growth/update?source=eastmoney&fund_code_list=000001,000002,000003,000004,000005"
    response = requests.post(url)
    print(f"更新基金历史涨幅状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"响应内容: {response.json()}")
    return response.status_code == 200

def test_get_fund_companies():
    """测试获取基金公司列表"""
    print("\n测试获取基金公司列表...")
    url = f"{BASE_URL}/fund/companies?page=1&page_size=5"
    response = requests.get(url)
    print(f"获取基金公司列表状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"响应内容: {response.json()}")
    return response.status_code == 200

def test_get_fund_with_growth():
    """测试获取带历史涨幅的基金详情"""
    print("\n测试获取带历史涨幅的基金详情...")
    # 先获取一个基金ID
    fund_list_url = f"{BASE_URL}/fund/?page=1&page_size=1"
    fund_response = requests.get(fund_list_url)
    if fund_response.status_code != 200:
        print("获取基金列表失败，无法测试基金历史涨幅")
        return False
    
    fund_data = fund_response.json()
    if not fund_data.get("data"):
        print("没有找到基金数据，无法测试基金历史涨幅")
        return False
    
    fund_id = fund_data["data"][0]["id"]
    print(f"使用基金ID: {fund_id} 测试历史涨幅")
    
    # 获取基金历史涨幅
    growth_url = f"{BASE_URL}/fund/{fund_id}/growth"
    growth_response = requests.get(growth_url)
    print(f"获取基金历史涨幅状态码: {growth_response.status_code}")
    if growth_response.status_code == 200:
        print(f"响应内容: {growth_response.json()}")
    return growth_response.status_code == 200

def main():
    """运行所有测试"""
    print("开始测试新功能...")
    
    # 测试API健康状态
    if not test_api_health():
        print("API健康检查失败，停止测试")
        return False
    
    # 测试导入基金列表
    if not test_import_funds():
        print("导入基金列表失败，停止测试")
        return False
    
    # 等待数据导入完成
    print("\n等待3秒，让数据导入完成...")
    time.sleep(3)
    
    # 测试获取基金列表
    if not test_get_funds():
        print("获取基金列表失败")
    
    # 测试更新基金历史涨幅
    if not test_update_fund_growth():
        print("更新基金历史涨幅失败")
    
    # 等待涨幅数据更新完成
    print("\n等待3秒，让涨幅数据更新完成...")
    time.sleep(3)
    
    # 测试获取基金公司列表
    if not test_get_fund_companies():
        print("获取基金公司列表失败")
    
    # 测试获取带历史涨幅的基金详情
    if not test_get_fund_with_growth():
        print("获取带历史涨幅的基金详情失败")
    
    print("\n所有测试完成！")
    return True

if __name__ == "__main__":
    main()
