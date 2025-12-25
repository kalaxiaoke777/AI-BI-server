#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试修复后的get_all_fund_data方法
"""

from app.scrapers.eastmoney import EastMoneyScraper

if __name__ == "__main__":
    scraper = EastMoneyScraper()
    
    print("测试get_all_fund_data方法...")
    fund_list = scraper.get_all_fund_data()
    print(f"成功获取 {len(fund_list)} 个基金")
    
    if fund_list:
        print("前5个基金数据:")
        for fund in fund_list[:5]:
            print(fund)
    else:
        print("未获取到基金数据")
