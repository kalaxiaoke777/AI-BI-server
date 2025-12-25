#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
直接测试基金排行数据更新功能
"""

import sys
import os
from datetime import datetime

# 添加项目根目录到PYTHONPATH
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.services.scrape_service import ScrapeService
from app.scrapers.enums import DataSource

def direct_test_fund_update():
    """直接测试基金排行数据更新功能"""
    print("开始直接测试基金排行数据更新功能...")
    
    try:
        # 创建ScrapeService实例
        service = ScrapeService()
        
        # 测试更新基金排行数据（只更新1页）
        result = service.update_fund_rank(source=DataSource.EASTMONEY, max_pages=1)
        
        print(f"\n更新结果: {result}")
        
        if result.get("status") == "error":
            print(f"更新失败: {result.get('message')}")
            return False
        
        print("\n更新成功！")
        print(f"成功数量: {result.get('success_count', 0)}")
        print(f"失败数量: {result.get('failed_count', 0)}")
        print(f"总数量: {result.get('total_count', 0)}")
        
        return True
    except Exception as e:
        print(f"测试失败: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = direct_test_fund_update()
    sys.exit(0 if success else 1)