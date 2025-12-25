#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 max_pages 参数是否生效
"""

import requests
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# API 地址
BASE_URL = "http://localhost:8000/api/v1/fund"


def test_rank_import_max_pages():
    """测试 rank/import 接口的 max_pages 参数"""
    logger.info("测试 rank/import 接口的 max_pages 参数")
    
    # 测试不同的 max_pages 值
    for max_pages in [1, 2, 3]:
        logger.info(f"测试 max_pages={max_pages}")
        
        # 构建请求 URL
        url = f"{BASE_URL}/rank/import"
        params = {
            "source": "eastmoney",
            "max_pages": max_pages
        }
        
        try:
            # 发送请求
            response = requests.post(url, params=params)
            response.raise_for_status()
            
            # 解析响应
            result = response.json()
            logger.info(f"max_pages={max_pages}，响应结果: {result}")
            
            # 检查状态
            if result["status"] == "success":
                logger.info(f"✓ max_pages={max_pages} 测试成功")
            else:
                logger.error(f"✗ max_pages={max_pages} 测试失败: {result.get('message', '未知错误')}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ max_pages={max_pages} 请求失败: {str(e)}")
        except Exception as e:
            logger.error(f"✗ max_pages={max_pages} 处理失败: {str(e)}")
        
        logger.info("-" * 50)


def test_rank_update_max_pages():
    """测试 rank/update 接口的 max_pages 参数"""
    logger.info("测试 rank/update 接口的 max_pages 参数")
    
    # 测试不同的 max_pages 值
    for max_pages in [1, 2]:
        logger.info(f"测试 max_pages={max_pages}")
        
        # 构建请求 URL
        url = f"{BASE_URL}/rank/update"
        params = {
            "source": "eastmoney",
            "max_pages": max_pages
        }
        
        try:
            # 发送请求
            response = requests.post(url, params=params)
            response.raise_for_status()
            
            # 解析响应
            result = response.json()
            logger.info(f"max_pages={max_pages}，响应结果: {result}")
            
            # 检查状态
            if result["status"] == "success":
                logger.info(f"✓ max_pages={max_pages} 测试成功")
            else:
                logger.error(f"✗ max_pages={max_pages} 测试失败: {result.get('message', '未知错误')}")
                
        except requests.exceptions.RequestException as e:
            logger.error(f"✗ max_pages={max_pages} 请求失败: {str(e)}")
        except Exception as e:
            logger.error(f"✗ max_pages={max_pages} 处理失败: {str(e)}")
        
        logger.info("-" * 50)


if __name__ == "__main__":
    logger.info("开始测试 max_pages 参数...")
    
    # 首先启动服务器
    logger.info("请确保服务器已启动: http://localhost:8000")
    logger.info("按 Enter 键继续...")
    input()
    
    # 测试 rank/import 接口
    test_rank_import_max_pages()
    
    # 测试 rank/update 接口
    test_rank_update_max_pages()
    
    logger.info("测试完成！")
