from fastapi import APIRouter, HTTPException, Depends
from loguru import logger
from typing import List, Optional
from sqlalchemy.orm import Session
from db import get_db
from app.services.scrape_service import ScrapeService
from app.scrapers.base import DataType, DataSource
from pydantic import BaseModel

router = APIRouter()

# 请求模型
class ScrapeRequest(BaseModel):
    source: str  # 数据来源，如eastmoney
    data_type: str  # 数据类型，如fund_basic
    fund_code_list: List[str]  # 基金代码列表

@router.post("/funds")
async def trigger_fund_scrape(request: ScrapeRequest, db: Session = Depends(get_db)):
    """触发基金数据采集"""
    logger.info(f"触发基金数据采集请求，数据源: {request.source}，数据类型: {request.data_type}，基金数量: {len(request.fund_code_list)}")
    
    try:
        # 转换数据源和数据类型为枚举
        source = DataSource(request.source)
        data_type = DataType(request.data_type)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 创建并运行采集任务
        task_id = scrape_service.create_scrape_task(source, data_type, request.fund_code_list)
        result = scrape_service.run_scrape_task(task_id)
        
        return {
            "status": result["status"],
            "message": result["message"],
            "task_id": task_id,
            "success_count": result.get("success_count", 0),
            "error_count": result.get("error_count", 0)
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"触发采集任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"触发采集任务失败: {str(e)}")

@router.post("/funds/all")
async def trigger_scrape_all_funds(source: str, data_type: str, db: Session = Depends(get_db)):
    """触发采集所有基金的数据"""
    logger.info(f"触发采集所有基金请求，数据源: {source}，数据类型: {data_type}")
    
    try:
        # 转换数据源和数据类型为枚举
        source_enum = DataSource(source)
        data_type_enum = DataType(data_type)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 创建并运行采集所有基金的任务
        task_id = scrape_service.create_scrape_all_funds_task(source_enum, data_type_enum)
        if not task_id:
            raise Exception("创建采集所有基金任务失败")
        
        result = scrape_service.run_scrape_task(task_id)
        
        return {
            "status": result["status"],
            "message": result["message"],
            "task_id": task_id,
            "success_count": result.get("success_count", 0),
            "error_count": result.get("error_count", 0)
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"触发采集所有基金任务失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"触发采集所有基金任务失败: {str(e)}")

@router.get("/status/{task_id}")
async def get_scrape_status(task_id: str, db: Session = Depends(get_db)):
    """获取采集任务状态"""
    logger.info(f"获取采集任务状态，任务ID: {task_id}")
    
    try:
        scrape_service = ScrapeService(db)
        status = scrape_service.get_scrape_task_status(task_id)
        
        if status["status"] == "error":
            raise HTTPException(status_code=404, detail=status["message"])
        
        return {
            "status": "success",
            "task": status
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取任务状态失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取任务状态失败: {str(e)}")

@router.get("/history")
async def get_scrape_history(
    page: int = 1,
    page_size: int = 10,
    source: Optional[str] = None,
    data_type: Optional[str] = None,
    status: Optional[str] = None,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """获取采集历史记录"""
    logger.info(f"获取采集历史记录，页码: {page}, 每页大小: {page_size}, 数据源: {source}, 数据类型: {data_type}, 状态: {status}, 开始日期: {start_date}, 结束日期: {end_date}")
    
    try:
        # 构建过滤条件
        filters = {}
        if source:
            filters["source"] = DataSource(source)
        if data_type:
            filters["data_type"] = DataType(data_type)
        if status:
            filters["status"] = status
        if start_date:
            filters["start_date"] = start_date
        if end_date:
            filters["end_date"] = end_date
        
        scrape_service = ScrapeService(db)
        history = scrape_service.get_scrape_history(page, page_size, **filters)
        
        return {
            "status": "success",
            "data": history
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"获取采集历史记录失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取采集历史记录失败: {str(e)}")
