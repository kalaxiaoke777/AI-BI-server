#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试基金排行数据更新功能
"""

import sys
import os
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

def test_update_fund_rank():
    """测试更新基金排行数据"""
    print("\n测试更新基金排行数据...")
    url = f"{BASE_URL}/fund/rank/update?source=eastmoney&max_pages=2"
    response = requests.post(url)
    print(f"更新基金排行数据状态码: {response.status_code}")
    if response.status_code == 200:
        print(f"响应内容: {response.json()}")
    return response.status_code == 200

def main():
    """运行所有测试"""
    print("开始测试基金排行数据更新功能...")
    
    # 测试API健康状态
    if not test_api_health():
        print("API健康检查失败，停止测试")
        return False
    
    # 测试更新基金排行数据
    if not test_update_fund_rank():
        print("更新基金排行数据失败")
    
    print("\n所有测试完成！")
    return True

if __name__ == "__main__":
    main()
