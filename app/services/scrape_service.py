from typing import List, Dict, Any
from uuid import uuid4
from datetime import datetime
from loguru import logger
from sqlalchemy.orm import Session
from app.scrapers.eastmoney import EastMoneyScraper
from app.scrapers.base import DataType, DataSource
from db.models import RawFundData, ScrapeTask, ScrapeTaskItem
from db import models

class ScrapeService:
    """数据采集服务"""
    
    def __init__(self, db: Session):
        self.db = db
        self.scrapers = {
            DataSource.EASTMONEY: EastMoneyScraper()
            # 可以添加更多爬虫，如天天基金、雪球等
        }
        self.logger = logger
        
    def update_fund_growth(self, source: DataSource, fund_code_list: List[str] = None) -> Dict[str, Any]:
        """更新基金历史涨幅数据
        
        Args:
            source: 数据来源
            fund_code_list: 基金代码列表，为空则更新所有基金
            
        Returns:
            Dict[str, Any]: 更新结果，包括成功数量、失败数量和总数量
        """
        self.logger.info(f"开始更新基金历史涨幅数据，数据源: {source}")
        
        scraper = self.scrapers.get(source)
        if not scraper:
            self.logger.error(f"未找到对应的爬虫，数据源: {source}")
            return {"status": "error", "message": "未找到对应的爬虫"}
        
        if not hasattr(scraper, "get_fund_growth_data"):
            self.logger.error(f"爬虫 {source} 不支持获取基金历史涨幅数据")
            return {"status": "error", "message": "爬虫不支持获取基金历史涨幅数据"}
        
        # 确定要更新的基金列表
        if not fund_code_list:
            self.logger.info("更新所有基金的历史涨幅数据")
            fund_code_list = [fund.fund_code for fund in self.db.query(models.FundBasic).all()]
        
        if not fund_code_list:
            self.logger.error("没有找到要更新的基金")
            return {"status": "error", "message": "没有找到要更新的基金"}
        
        # 处理基金历史涨幅数据
        success_count = 0
        failed_count = 0
        total_count = len(fund_code_list)
        
        from datetime import datetime
        current_date = datetime.now()
        
        for fund_code in fund_code_list:
            try:
                # 获取基金
                fund = self.db.query(models.FundBasic).filter(
                    models.FundBasic.fund_code == fund_code
                ).first()
                
                if not fund:
                    self.logger.error(f"基金不存在，基金代码: {fund_code}")
                    failed_count += 1
                    continue
                
                # 获取涨幅数据
                growth_data = scraper.get_fund_growth_data(fund_code)
                
                if not growth_data:
                    self.logger.error(f"获取涨幅数据失败，基金代码: {fund_code}")
                    failed_count += 1
                    continue
                
                # 查找现有涨幅数据
                existing_growth = self.db.query(models.FundGrowth).filter(
                    models.FundGrowth.fund_id == fund.id,
                    models.FundGrowth.update_date == current_date.date()
                ).first()
                
                # 创建或更新涨幅数据
                if existing_growth:
                    # 更新现有涨幅数据
                    for growth_item in growth_data:
                        if growth_item["growth_type"] == "近1日":
                            existing_growth.daily_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近1周":
                            existing_growth.weekly_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近1月":
                            existing_growth.monthly_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近3月":
                            existing_growth.quarterly_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近1年":
                            existing_growth.yearly_growth = growth_item["growth_value"]
                    existing_growth.updated_at = current_date
                else:
                    # 创建新涨幅数据
                    new_growth = models.FundGrowth(
                        fund_id=fund.id,
                        update_date=current_date
                    )
                    
                    # 填充涨幅数据
                    for growth_item in growth_data:
                        if growth_item["growth_type"] == "近1日":
                            new_growth.daily_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近1周":
                            new_growth.weekly_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近1月":
                            new_growth.monthly_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近3月":
                            new_growth.quarterly_growth = growth_item["growth_value"]
                        elif growth_item["growth_type"] == "近1年":
                            new_growth.yearly_growth = growth_item["growth_value"]
                    
                    self.db.add(new_growth)
                
                self.db.commit()
                success_count += 1
                self.logger.info(f"更新基金历史涨幅数据成功，基金代码: {fund_code}")
                
            except Exception as e:
                self.logger.error(f"更新基金历史涨幅数据失败，基金代码: {fund_code}，错误: {str(e)}")
                self.db.rollback()
                failed_count += 1
        
        self.logger.info(f"基金历史涨幅数据更新完成，数据源: {source}，总数量: {total_count}，成功: {success_count}，失败: {failed_count}")
        
        return {
            "status": "success",
            "total_count": total_count,
            "success_count": success_count,
            "failed_count": failed_count
        }
    
    def update_fund_rank(self, source: DataSource, max_pages: int = None) -> Dict[str, Any]:
        """更新基金排行数据
        
        Args:
            source: 数据来源
            max_pages: 最大页码，为None时获取所有数据，默认为None
            
        Returns:
            Dict[str, Any]: 更新结果，包括成功数量、失败数量和总数量
        """
        self.logger.info(f"开始更新基金排行数据，数据源: {source}, max_pages: {max_pages}")
        
        scraper = self.scrapers.get(source)
        if not scraper:
            self.logger.error(f"未找到对应的爬虫，数据源: {source}")
            return {"status": "error", "message": "未找到对应的爬虫"}
        
        if not hasattr(scraper, "get_all_fund_rank_data"):
            self.logger.error(f"爬虫 {source} 不支持获取基金排行数据")
            return {"status": "error", "message": "爬虫不支持获取基金排行数据"}
        
        # 获取基金排行数据
        fund_rank_data = scraper.get_all_fund_rank_data(max_pages)
        
        if not fund_rank_data:
            self.logger.error(f"获取基金排行数据失败，数据源: {source}")
            return {"status": "error", "message": "获取基金排行数据失败"}
        
        # 处理基金排行数据
        success_count = 0
        failed_count = 0
        total_count = len(fund_rank_data)
        
        from datetime import datetime
        current_date = datetime.now()
        
        for rank, fund_data in enumerate(fund_rank_data, 1):
            try:
                # 查找基金
                fund = self.db.query(models.FundBasic).filter(
                    models.FundBasic.fund_code == fund_data["fund_code"]
                ).first()
                
                if not fund:
                    # 基金不存在，创建新记录
                    self.logger.info(f"基金不存在，创建新基金，基金代码: {fund_data['fund_code']}")
                    new_fund = models.FundBasic(
                        fund_code=fund_data["fund_code"],
                        short_name=fund_data.get("short_name", ""),
                        fund_name=fund_data["fund_name"],
                        fund_type=fund_data.get("fund_type"),
                        latest_nav=fund_data["nav"],
                        is_purchaseable=True,  # 默认设置为可购买
                        risk_level=fund_data.get("risk_level"),
                        purchase_fee=fund_data.get("purchase_fee"),
                        redemption_fee=fund_data.get("redemption_fee"),
                        purchase_fee_rate=fund_data.get("purchase_fee_rate")
                    )
                    # 处理成立日期
                    if fund_data.get("launch_date"):
                        try:
                            new_fund.launch_date = datetime.strptime(fund_data["launch_date"], "%Y-%m-%d").date()
                        except ValueError:
                            pass
                    self.db.add(new_fund)
                    self.db.commit()
                    fund = new_fund
                else:
                    # 更新现有基金基本信息
                    fund.short_name = fund_data.get("short_name", fund.short_name)
                    fund.fund_type = fund_data.get("fund_type", fund.fund_type)
                    if fund_data["nav"] is not None:
                        fund.latest_nav = fund_data["nav"]
                    # 更新新增字段
                    if fund_data.get("risk_level") is not None:
                        fund.risk_level = fund_data.get("risk_level")
                    if fund_data.get("purchase_fee") is not None:
                        fund.purchase_fee = fund_data.get("purchase_fee")
                    if fund_data.get("redemption_fee") is not None:
                        fund.redemption_fee = fund_data.get("redemption_fee")
                    if fund_data.get("purchase_fee_rate") is not None:
                        fund.purchase_fee_rate = fund_data.get("purchase_fee_rate")
                    # 更新成立日期
                    if fund_data.get("launch_date"):
                        try:
                            fund.launch_date = datetime.strptime(fund_data["launch_date"], "%Y-%m-%d").date()
                        except ValueError:
                            pass
                
                # 记录排行数据 - 增量更新，不删除原有数据
                # 查找现有排行数据
                existing_rank = self.db.query(models.FundRank).filter(
                    models.FundRank.fund_id == fund.id,
                    models.FundRank.rank_date == current_date.date()
                ).first()
                
                if existing_rank:
                    # 更新现有排行数据
                    existing_rank.rank = rank
                    existing_rank.rank_type = "daily_rank"  # 默认日排行，可根据实际情况调整
                    existing_rank.nav = fund_data["nav"]
                    existing_rank.accum_nav = fund_data.get("accum_nav")
                    existing_rank.daily_growth = fund_data.get("daily_growth")
                    existing_rank.weekly_growth = fund_data.get("weekly_growth")
                    existing_rank.monthly_growth = fund_data.get("monthly_growth")
                    existing_rank.quarterly_growth = fund_data.get("quarterly_growth")
                    existing_rank.yearly_growth = fund_data.get("yearly_growth")
                    existing_rank.two_year_growth = fund_data.get("two_year_growth")
                    existing_rank.three_year_growth = fund_data.get("three_year_growth")
                    existing_rank.five_year_growth = fund_data.get("five_year_growth")
                    existing_rank.ytd_growth = fund_data.get("ytd_growth")
                    existing_rank.since_launch_growth = fund_data.get("since_launch_growth")
                    existing_rank.updated_at = current_date
                else:
                    # 创建新排行数据
                    new_rank = models.FundRank(
                        fund_id=fund.id,
                        rank_date=current_date,
                        rank=rank,
                        rank_type="daily_rank",  # 默认日排行，可根据实际情况调整
                        nav=fund_data["nav"],
                        accum_nav=fund_data.get("accum_nav"),
                        daily_growth=fund_data.get("daily_growth"),
                        weekly_growth=fund_data.get("weekly_growth"),
                        monthly_growth=fund_data.get("monthly_growth"),
                        quarterly_growth=fund_data.get("quarterly_growth"),
                        yearly_growth=fund_data.get("yearly_growth"),
                        two_year_growth=fund_data.get("two_year_growth"),
                        three_year_growth=fund_data.get("three_year_growth"),
                        five_year_growth=fund_data.get("five_year_growth"),
                        ytd_growth=fund_data.get("ytd_growth"),
                        since_launch_growth=fund_data.get("since_launch_growth")
                    )
                    self.db.add(new_rank)
                
                # 更新涨幅数据
                # 查找现有涨幅数据
                existing_growth = self.db.query(models.FundGrowth).filter(
                    models.FundGrowth.fund_id == fund.id,
                    models.FundGrowth.update_date == current_date.date()
                ).first()
                
                if existing_growth:
                    # 更新现有涨幅数据
                    existing_growth.daily_growth = fund_data.get("daily_growth")
                    existing_growth.weekly_growth = fund_data.get("weekly_growth")
                    existing_growth.monthly_growth = fund_data.get("monthly_growth")
                    existing_growth.quarterly_growth = fund_data.get("quarterly_growth")
                    existing_growth.yearly_growth = fund_data.get("yearly_growth")
                    existing_growth.updated_at = current_date
                else:
                    # 创建新涨幅数据
                    new_growth = models.FundGrowth(
                        fund_id=fund.id,
                        daily_growth=fund_data.get("daily_growth"),
                        weekly_growth=fund_data.get("weekly_growth"),
                        monthly_growth=fund_data.get("monthly_growth"),
                        quarterly_growth=fund_data.get("quarterly_growth"),
                        yearly_growth=fund_data.get("yearly_growth"),
                        update_date=current_date
                    )
                    self.db.add(new_growth)
                
                self.db.commit()
                success_count += 1
                self.logger.info(f"更新基金排行数据成功，基金代码: {fund_data['fund_code']}")
                
            except Exception as e:
                self.logger.error(f"处理基金排行数据失败，基金代码: {fund_data['fund_code']}，错误: {str(e)}")
                self.db.rollback()
                failed_count += 1
        
        self.logger.info(f"基金排行数据更新完成，数据源: {source}，总数量: {total_count}，成功: {success_count}，失败: {failed_count}")
        
        return {
            "status": "success",
            "total_count": total_count,
            "success_count": success_count,
            "failed_count": failed_count
        }
    
    def import_fund_list(self, source: DataSource) -> Dict[str, Any]:
        """从指定数据源导入基金列表（仅初始化使用，不覆盖已有数据）
        
        Args:
            source: 数据来源
            
        Returns:
            Dict[str, Any]: 导入结果，包括新增数量、更新数量和总数量
        """
        self.logger.info(f"开始导入基金列表，数据源: {source}")
        
        scraper = self.scrapers.get(source)
        if not scraper:
            self.logger.error(f"未找到对应的爬虫，数据源: {source}")
            return {"status": "error", "message": "未找到对应的爬虫"}
        
        if not hasattr(scraper, "get_all_fund_data"):
            self.logger.error(f"爬虫 {source} 不支持获取完整基金数据")
            return {"status": "error", "message": "爬虫不支持获取完整基金数据"}
        
        # 获取基金数据
        fund_list = scraper.get_all_fund_data()
        if not fund_list:
            self.logger.error(f"获取基金数据失败，数据源: {source}")
            return {"status": "error", "message": "获取基金数据失败"}
        
        # 处理基金数据
        added_count = 0
        updated_count = 0
        total_count = len(fund_list)
        
        for fund_data in fund_list:
            try:
                # 查找现有基金
                existing_fund = self.db.query(models.FundBasic).filter(
                    models.FundBasic.fund_code == fund_data["fund_code"]
                ).first()
                
                # 处理基金公司信息（暂时使用公司名称作为关联，后续可扩展公司代码）
                # 注意：当前东方财富基金列表API返回的数据中没有公司代码，只有基金基本信息
                company_name = fund_data.get("company", "")
                
                if existing_fund:
                    # 基金已存在，不更新，直接跳过
                    self.logger.info(f"基金已存在，跳过，基金代码: {fund_data['fund_code']}")
                    continue
                else:
                    # 创建新基金
                    new_fund = models.FundBasic(
                        fund_code=fund_data["fund_code"],
                        short_name=fund_data["short_name"],
                        fund_name=fund_data["fund_name"],
                        fund_type=fund_data["fund_type"],
                        pinyin=fund_data["pinyin"],
                        company_name=company_name,
                        is_purchaseable=True,  # 默认设置为可购买
                        risk_level="未知"  # 默认风险等级
                    )
                    self.db.add(new_fund)
                    self.db.commit()
                    added_count += 1
                    
            except Exception as e:
                self.logger.error(f"处理基金数据失败，基金代码: {fund_data['fund_code']}，错误: {str(e)}")
                self.db.rollback()
        
        self.logger.info(f"基金列表导入完成，数据源: {source}，总数量: {total_count}，新增: {added_count}，更新: {updated_count}")
        
        return {
            "status": "success",
            "total_count": total_count,
            "added_count": added_count,
            "updated_count": updated_count
        }
    
    def import_fund_company_list(self, source: DataSource) -> Dict[str, Any]:
        """从指定数据源导入基金公司列表（仅初始化使用，不覆盖已有数据）
        
        Args:
            source: 数据来源
            
        Returns:
            Dict[str, Any]: 导入结果，包括新增数量、更新数量和总数量
        """
        self.logger.info(f"开始导入基金公司列表，数据源: {source}")
        
        scraper = self.scrapers.get(source)
        if not scraper:
            self.logger.error(f"未找到对应的爬虫，数据源: {source}")
            return {"status": "error", "message": "未找到对应的爬虫"}
        
        if not hasattr(scraper, "get_fund_company_list"):
            self.logger.error(f"爬虫 {source} 不支持获取基金公司列表")
            return {"status": "error", "message": "爬虫不支持获取基金公司列表"}
        
        # 获取基金公司数据
        company_list = scraper.get_fund_company_list()
        if not company_list:
            self.logger.error(f"获取基金公司数据失败，数据源: {source}")
            return {"status": "error", "message": "获取基金公司数据失败"}
        
        # 处理基金公司数据
        added_count = 0
        updated_count = 0
        total_count = len(company_list)
        
        for company_data in company_list:
            try:
                # 查找现有公司
                existing_company = self.db.query(models.FundCompany).filter(
                    models.FundCompany.company_code == company_data["company_code"]
                ).first()
                
                if existing_company:
                    # 公司已存在，不更新，直接跳过
                    self.logger.info(f"公司已存在，跳过，公司代码: {company_data['company_code']}")
                    continue
                else:
                    # 创建新公司
                    new_company = models.FundCompany(
                        company_code=company_data["company_code"],
                        company_name=company_data["company_name"],
                        short_name=company_data.get("short_name", company_data["company_name"]),  # 使用数据中的简称
                        establish_date=company_data.get("established_date"),  # 成立日期
                        # 其他字段暂时为空，后续可扩展
                    )
                    self.db.add(new_company)
                    self.db.commit()
                    added_count += 1
                    
            except Exception as e:
                self.logger.error(f"处理基金公司数据失败，公司代码: {company_data['company_code']}，错误: {str(e)}")
                self.db.rollback()
        
        self.logger.info(f"基金公司列表导入完成，数据源: {source}，总数量: {total_count}，新增: {added_count}，更新: {updated_count}")
        
        return {
            "status": "success",
            "total_count": total_count,
            "added_count": added_count,
            "updated_count": updated_count
        }
    
    def get_all_fund_codes(self, source: DataSource) -> List[str]:
        """获取所有基金代码列表
        
        Args:
            source: 数据来源
            
        Returns:
            List[str]: 基金代码列表
        """
        scraper = self.scrapers.get(source)
        if not scraper:
            self.logger.error(f"未找到对应的爬虫，数据源: {source}")
            return []
        
        # 检查爬虫是否有获取所有基金代码的方法
        if hasattr(scraper, "get_all_fund_codes"):
            return scraper.get_all_fund_codes()
        else:
            self.logger.error(f"爬虫 {source} 不支持获取所有基金代码")
            return []
    
    def create_scrape_all_funds_task(self, source: DataSource, data_type: DataType) -> str:
        """创建采集所有基金的任务
        
        Args:
            source: 数据来源
            data_type: 数据类型
            
        Returns:
            str: 任务ID
        """
        # 获取所有基金代码
        fund_code_list = self.get_all_fund_codes(source)
        if not fund_code_list:
            self.logger.error(f"获取基金代码列表失败，数据源: {source}")
            return None
        
        # 创建采集任务
        return self.create_scrape_task(source, data_type, fund_code_list)
    
    def create_scrape_task(self, source: DataSource, data_type: DataType, fund_code_list: List[str]) -> str:
        """创建采集任务
        
        Args:
            source: 数据来源
            data_type: 数据类型
            fund_code_list: 基金代码列表
            
        Returns:
            str: 任务ID
        """
        task_id = str(uuid4())
        
        # 创建任务记录
        task = ScrapeTask(
            task_id=task_id,
            source=source,
            data_type=data_type,
            status="pending",
            total_count=len(fund_code_list),
            success_count=0,
            error_count=0
        )
        self.db.add(task)
        self.db.flush()
        
        # 创建任务项记录
        for fund_code in fund_code_list:
            task_item = ScrapeTaskItem(
                task_id=task.id,
                fund_code=fund_code,
                status="pending"
            )
            self.db.add(task_item)
        
        self.db.commit()
        
        logger.info(f"创建采集任务成功，任务ID: {task_id}，数据源: {source}，数据类型: {data_type}，基金数量: {len(fund_code_list)}")
        return task_id
    
    def run_scrape_task(self, task_id: str) -> Dict[str, Any]:
        """运行采集任务
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict[str, Any]: 任务执行结果
        """
        # 查询任务
        task = self.db.query(ScrapeTask).filter(ScrapeTask.task_id == task_id).first()
        if not task:
            logger.error(f"任务不存在，任务ID: {task_id}")
            return {"status": "error", "message": "任务不存在"}
        
        # 更新任务状态为运行中
        task.status = "running"
        task.start_time = datetime.now()
        self.db.commit()
        
        # 查询任务项
        task_items = self.db.query(ScrapeTaskItem).filter(ScrapeTaskItem.task_id == task.id).all()
        fund_code_list = [item.fund_code for item in task_items]
        
        # 获取对应的爬虫
        scraper = self.scrapers.get(task.source)
        if not scraper:
            logger.error(f"未找到对应的爬虫，数据源: {task.source}")
            task.status = "failed"
            task.end_time = datetime.now()
            task.error_message = f"未找到对应的爬虫，数据源: {task.source}"
            self.db.commit()
            return {"status": "error", "message": "未找到对应的爬虫"}
        
        # 运行爬虫
        try:
            raw_data_list = scraper.run(
                fund_code_list=fund_code_list,
                data_type=task.data_type
            )
            
            # 处理抓取结果
            success_count = 0
            error_count = 0
            
            for raw_data in raw_data_list:
                try:
                    # 保存原始数据到数据库
                    self._save_raw_data(raw_data)
                    success_count += 1
                    
                    # 更新任务项状态
                    task_item = next((item for item in task_items if item.fund_code == raw_data.fund_code), None)
                    if task_item:
                        task_item.status = "success"
                        self.db.commit()
                
                except Exception as e:
                    logger.error(f"保存原始数据失败，基金代码: {raw_data.fund_code}，错误: {str(e)}")
                    error_count += 1
                    
                    # 更新任务项状态
                    task_item = next((item for item in task_items if item.fund_code == raw_data.fund_code), None)
                    if task_item:
                        task_item.status = "failed"
                        task_item.error_message = str(e)
                        self.db.commit()
            
            # 更新任务状态
            task.status = "completed"
            task.end_time = datetime.now()
            task.success_count = success_count
            task.error_count = error_count
            self.db.commit()
            
            logger.info(f"采集任务完成，任务ID: {task_id}，成功: {success_count}，失败: {error_count}")
            return {
                "status": "success",
                "message": "采集任务完成",
                "success_count": success_count,
                "error_count": error_count
            }
        
        except Exception as e:
            logger.error(f"采集任务失败，任务ID: {task_id}，错误: {str(e)}")
            task.status = "failed"
            task.end_time = datetime.now()
            task.error_message = str(e)
            self.db.commit()
            return {
                "status": "error",
                "message": f"采集任务失败: {str(e)}",
                "success_count": 0,
                "error_count": len(fund_code_list)
            }
    
    def _save_raw_data(self, raw_data: Any) -> None:
        """保存原始数据到数据库
        
        Args:
            raw_data: 原始数据对象
        """
        # 检查数据是否已存在（去重）
        existing_data = self.db.query(RawFundData).filter(
            RawFundData.fund_code == raw_data.fund_code,
            RawFundData.data_type == raw_data.data_type,
            RawFundData.source == raw_data.source,
            RawFundData.source_url == raw_data.source_url
        ).first()
        
        if existing_data:
            logger.info(f"原始数据已存在，跳过保存，基金代码: {raw_data.fund_code}")
            return
        
        # 创建原始数据记录
        db_raw_data = RawFundData(
            fund_code=raw_data.fund_code,
            data_type=raw_data.data_type,
            source=raw_data.source,
            source_url=raw_data.source_url,
            raw_content=raw_data.raw_content,
            is_processed=False
        )
        
        self.db.add(db_raw_data)
        self.db.commit()
        
        logger.info(f"保存原始数据成功，基金代码: {raw_data.fund_code}，数据类型: {raw_data.data_type}")
    
    def get_scrape_task_status(self, task_id: str) -> Dict[str, Any]:
        """获取采集任务状态
        
        Args:
            task_id: 任务ID
            
        Returns:
            Dict[str, Any]: 任务状态信息
        """
        task = self.db.query(ScrapeTask).filter(ScrapeTask.task_id == task_id).first()
        if not task:
            return {"status": "error", "message": "任务不存在"}
        
        task_items = self.db.query(ScrapeTaskItem).filter(ScrapeTaskItem.task_id == task.id).all()
        
        return {
            "task_id": task.task_id,
            "source": task.source.value,
            "data_type": task.data_type.value,
            "status": task.status,
            "start_time": task.start_time.isoformat() if task.start_time else None,
            "end_time": task.end_time.isoformat() if task.end_time else None,
            "total_count": task.total_count,
            "success_count": task.success_count,
            "error_count": task.error_count,
            "error_message": task.error_message,
            "items": [
                {
                    "fund_code": item.fund_code,
                    "status": item.status,
                    "error_message": item.error_message,
                    "created_at": item.created_at.isoformat(),
                    "updated_at": item.updated_at.isoformat()
                }
                for item in task_items
            ]
        }
    
    def get_scrape_history(self, page: int = 1, page_size: int = 10, **filters) -> Dict[str, Any]:
        """获取采集历史记录
        
        Args:
            page: 页码
            page_size: 每页大小
            filters: 过滤条件，如source、data_type、status等
            
        Returns:
            Dict[str, Any]: 历史记录列表
        """
        query = self.db.query(ScrapeTask)
        
        # 应用过滤条件
        if "source" in filters:
            query = query.filter(ScrapeTask.source == filters["source"])
        if "data_type" in filters:
            query = query.filter(ScrapeTask.data_type == filters["data_type"])
        if "status" in filters:
            query = query.filter(ScrapeTask.status == filters["status"])
        if "start_date" in filters:
            query = query.filter(ScrapeTask.created_at >= filters["start_date"])
        if "end_date" in filters:
            query = query.filter(ScrapeTask.created_at <= filters["end_date"])
        
        # 计算总数
        total = query.count()
        
        # 分页查询
        tasks = query.order_by(ScrapeTask.created_at.desc()).offset((page - 1) * page_size).limit(page_size).all()
        
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": [
                {
                    "task_id": task.task_id,
                    "source": task.source.value,
                    "data_type": task.data_type.value,
                    "status": task.status,
                    "start_time": task.start_time.isoformat() if task.start_time else None,
                    "end_time": task.end_time.isoformat() if task.end_time else None,
                    "total_count": task.total_count,
                    "success_count": task.success_count,
                    "error_count": task.error_count,
                    "created_at": task.created_at.isoformat()
                }
                for task in tasks
            ]
        }
