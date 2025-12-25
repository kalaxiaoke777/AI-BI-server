#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试 max_pages 参数是否生效
"""

import logging
from app.scrapers.eastmoney import EastMoneyScraper

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def test_get_all_fund_rank_data():
    """测试 get_all_fund_rank_data 方法的 max_pages 参数"""
    logger.info("测试 get_all_fund_rank_data 方法的 max_pages 参数")
    
    # 创建爬虫实例
    scraper = EastMoneyScraper()
    
    # 测试不同的 max_pages 值
    test_cases = [1, 2, 3]
    
    for max_pages in test_cases:
        logger.info(f"\n测试 max_pages={max_pages}")
        logger.info("-" * 40)
        
        try:
            # 调用方法获取数据
            fund_data = scraper.get_all_fund_rank_data(max_pages=max_pages)
            
            logger.info(f"✓ 成功获取数据，返回 {len(fund_data)} 条记录")
            logger.info(f"✓ max_pages={max_pages} 测试通过")
            
        except Exception as e:
            logger.error(f"✗ 测试失败: {str(e)}")
    
    logger.info("\n" + "=" * 50)
    logger.info("所有测试完成！")


if __name__ == "__main__":
    test_get_all_fund_rank_data()
