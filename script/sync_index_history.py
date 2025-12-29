#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
同步指数历史数据到数据库
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime, timedelta
from loguru import logger
from db import models, init_db, SessionLocal
from app.scrapers.eastmoney_index import EastMoneyIndexScraper

def init_index_info():
    """初始化指数信息到数据库"""
    logger.info("初始化指数信息...")
    
    # 指数代码映射（东方财富格式）
    index_mapping = {
        "沪深300": {
            "index_code": "000300",
            "secid": "1.000300",
            "market": "沪市",
            "description": "沪深300指数是由上海和深圳证券市场中选取300只A股作为样本，反映A股市场整体走势的指数"
        },
        "中证500": {
            "index_code": "000905",
            "secid": "1.000905",
            "market": "沪市",
            "description": "中证500指数是由全部A股中剔除沪深300指数成份股及总市值排名前300名的股票后，总市值排名靠前的500只股票组成"
        },
        "中证1000": {
            "index_code": "000852",
            "secid": "1.000852",
            "market": "沪市",
            "description": "中证1000指数是由全部A股中剔除沪深300和中证500指数成份股后，总市值排名靠前的1000只股票组成"
        },
        "创业板指": {
            "index_code": "399006",
            "secid": "0.399006",
            "market": "深市",
            "description": "创业板指数是反映创业板市场走势的核心指数，由最具代表性的100家创业板上市企业股票组成"
        },
        "科创50": {
            "index_code": "000688",
            "secid": "1.000688",
            "market": "沪市",
            "description": "科创板50指数由上海证券交易所科创板中市值大、流动性好的50只证券组成，反映最具市场代表性的一批科创企业的整体表现"
        }
    }
    
    db = SessionLocal()
    try:
        for index_name, index_info in index_mapping.items():
            # 检查指数是否已存在
            existing = db.query(models.IndexInfo).filter_by(index_name=index_name).first()
            if existing:
                logger.info(f"指数 {index_name} 已存在，跳过")
                continue
            
            # 创建新的指数信息
            new_index = models.IndexInfo(
                index_name=index_name,
                index_code=index_info["index_code"],
                secid=index_info["secid"],
                market=index_info["market"],
                description=index_info["description"]
            )
            db.add(new_index)
            logger.info(f"添加指数: {index_name}")
        
        db.commit()
        logger.info("指数信息初始化完成")
    except Exception as e:
        logger.error(f"初始化指数信息失败: {str(e)}")
        db.rollback()
        raise
    finally:
        db.close()

def sync_index_history(index_name: str = None, start_date: str = None, end_date: str = None):
    """同步指数历史数据到数据库
    
    Args:
        index_name: 指数名称，如果为None则同步所有指数
        start_date: 开始日期，格式：YYYY-MM-DD
        end_date: 结束日期，格式：YYYY-MM-DD
    """
    logger.info("开始同步指数历史数据...")
    
    # 初始化爬虫
    scraper = EastMoneyIndexScraper()
    
    # 获取指数信息
    db = SessionLocal()
    try:
        if index_name:
            index_list = db.query(models.IndexInfo).filter_by(index_name=index_name).all()
            if not index_list:
                logger.error(f"未找到指数: {index_name}")
                return False
        else:
            index_list = db.query(models.IndexInfo).all()
        
        # 设置默认日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # 默认获取最近5年数据
            start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")
        
        logger.info(f"同步日期范围: {start_date} 到 {end_date}")
        
        for index_info in index_list:
            logger.info(f"开始同步 {index_info.index_name} 的历史数据...")
            
            # 从东方财富API获取历史数据
            history_data = scraper.get_index_history(index_info.index_name, start_date, end_date)
            if not history_data:
                logger.error(f"获取 {index_info.index_name} 历史数据失败")
                continue
            
            logger.info(f"成功获取 {index_info.index_name} {len(history_data)} 条历史数据")
            
            # 保存到数据库
            saved_count = 0
            updated_count = 0
            skipped_count = 0
            
            for data in history_data:
                # 转换日期格式
                trade_date = datetime.strptime(data["date"], "%Y-%m-%d")
                
                # 检查数据是否已存在
                existing = db.query(models.IndexHistory).filter(
                    models.IndexHistory.index_id == index_info.id,
                    models.IndexHistory.trade_date == trade_date
                ).first()
                
                if existing:
                    # 数据已存在，更新
                    existing.open = data["open"]
                    existing.close = data["close"]
                    existing.high = data["high"]
                    existing.low = data["low"]
                    existing.volume = data["volume"]
                    existing.amount = data["amount"]
                    updated_count += 1
                else:
                    # 新数据，插入
                    new_history = models.IndexHistory(
                        index_id=index_info.id,
                        trade_date=trade_date,
                        open=data["open"],
                        close=data["close"],
                        high=data["high"],
                        low=data["low"],
                        volume=data["volume"],
                        amount=data["amount"]
                    )
                    db.add(new_history)
                    saved_count += 1
                
                # 每100条提交一次
                if (saved_count + updated_count) % 100 == 0:
                    db.commit()
                    logger.info(f"已处理 {index_info.index_name} {saved_count + updated_count} 条数据")
            
            # 提交剩余数据
            db.commit()
            
            logger.info(f"{index_info.index_name} 同步完成: 新增 {saved_count} 条，更新 {updated_count} 条，跳过 {skipped_count} 条")
        
        logger.info("所有指数历史数据同步完成")
        return True
    
    except Exception as e:
        logger.error(f"同步指数历史数据失败: {str(e)}")
        import traceback
        traceback.print_exc()
        db.rollback()
        return False
    finally:
        db.close()

def export_index_history_to_csv(index_name: str = None, file_path: str = None):
    """将指数历史数据导出到CSV文件
    
    Args:
        index_name: 指数名称，如果为None则导出所有指数
        file_path: 输出文件路径，如果为None则使用默认名称
    """
    logger.info("开始导出指数历史数据到CSV...")
    
    db = SessionLocal()
    try:
        if index_name:
            index_list = db.query(models.IndexInfo).filter_by(index_name=index_name).all()
            if not index_list:
                logger.error(f"未找到指数: {index_name}")
                return False
        else:
            index_list = db.query(models.IndexInfo).all()
        
        for index_info in index_list:
            logger.info(f"开始导出 {index_info.index_name} 的历史数据...")
            
            # 查询历史数据
            history_data = db.query(models.IndexHistory).filter(
                models.IndexHistory.index_id == index_info.id
            ).order_by(models.IndexHistory.trade_date).all()
            
            if not history_data:
                logger.error(f"未找到 {index_info.index_name} 的历史数据")
                continue
            
            # 转换为DataFrame
            data_list = []
            for item in history_data:
                data_list.append({
                    "date": item.trade_date.strftime("%Y-%m-%d"),
                    "open": item.open,
                    "close": item.close,
                    "high": item.high,
                    "low": item.low,
                    "volume": item.volume,
                    "amount": item.amount
                })
            
            df = pd.DataFrame(data_list)
            
            # 保存到CSV
            if file_path:
                save_path = file_path
            else:
                save_path = f"{index_info.index_name}_history.csv"
            
            df.to_csv(save_path, index=False, encoding="utf-8-sig")
            logger.info(f"{index_info.index_name} 历史数据已导出到: {save_path}")
        
        logger.info("所有指数历史数据导出完成")
        return True
    
    except Exception as e:
        logger.error(f"导出指数历史数据失败: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        db.close()

def main():
    """主函数"""
    logger.info("=== 指数历史数据同步脚本 ===")
    
    # 1. 初始化数据库表
    logger.info("1. 初始化数据库表...")
    init_db()
    
    # 2. 初始化指数信息
    logger.info("\n2. 初始化指数信息...")
    init_index_info()
    
    # 3. 同步指数历史数据
    logger.info("\n3. 同步指数历史数据...")
    sync_index_history()
    
    # 4. 导出到CSV文件
    logger.info("\n4. 导出指数历史数据到CSV文件...")
    export_index_history_to_csv()
    
    logger.info("\n=== 脚本执行完成 ===")

if __name__ == "__main__":
    main()
