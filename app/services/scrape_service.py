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
        
    def import_fund_list(self, source: DataSource) -> Dict[str, Any]:
        """从指定数据源导入基金列表
        
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
                
                if existing_fund:
                    # 更新现有基金
                    existing_fund.short_name = fund_data["short_name"]
                    existing_fund.fund_name = fund_data["fund_name"]
                    existing_fund.fund_type = fund_data["fund_type"]
                    existing_fund.pinyin = fund_data["pinyin"]
                    self.db.commit()
                    updated_count += 1
                else:
                    # 创建新基金
                    new_fund = models.FundBasic(
                        fund_code=fund_data["fund_code"],
                        short_name=fund_data["short_name"],
                        fund_name=fund_data["fund_name"],
                        fund_type=fund_data["fund_type"],
                        pinyin=fund_data["pinyin"]
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
