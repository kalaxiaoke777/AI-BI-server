from fastapi import APIRouter, HTTPException, Depends, Query
from loguru import logger
from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from db import get_db
from db import models
from app.services.scrape_service import ScrapeService
from app.scrapers.base import DataSource

router = APIRouter()

@router.get("/")
async def get_funds(
    page: int = 1,
    page_size: int = 10,
    fund_code: Optional[str] = None,
    fund_name: Optional[str] = None,
    fund_type: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """获取基金列表"""
    logger.info(f"获取基金列表，页码: {page}, 每页大小: {page_size}, 基金代码: {fund_code}, 基金名称: {fund_name}, 基金类型: {fund_type}")
    
    # 构建查询
    query = db.query(models.FundBasic)
    
    # 应用过滤条件
    if fund_code:
        query = query.filter(models.FundBasic.fund_code.like(f"%{fund_code}%"))
    if fund_name:
        query = query.filter(models.FundBasic.fund_name.like(f"%{fund_name}%"))
    if fund_type:
        query = query.filter(models.FundBasic.fund_type.like(f"%{fund_type}%"))
    
    # 计算总数
    total = query.count()
    
    # 分页查询
    funds = query.offset((page - 1) * page_size).limit(page_size).all()
    
    # 转换为响应格式
    data = []
    for fund in funds:
        data.append({
            "id": fund.id,
            "fund_code": fund.fund_code,
            "short_name": fund.short_name,
            "fund_name": fund.fund_name,
            "fund_type": fund.fund_type,
            "pinyin": fund.pinyin,
            "manager": fund.manager,
            "company": fund.company,
            "establish_date": fund.establish_date.isoformat() if fund.establish_date else None,
            "latest_nav": fund.latest_nav,
            "latest_nav_date": fund.latest_nav_date.isoformat() if fund.latest_nav_date else None,
            "created_at": fund.created_at.isoformat(),
            "updated_at": fund.updated_at.isoformat() if fund.updated_at else None
        })
    
    return {
        "total": total,
        "page": page,
        "page_size": page_size,
        "data": data
    }

@router.get("/{fund_id}")
async def get_fund_detail(fund_id: int, db: Session = Depends(get_db)):
    """获取基金详情"""
    logger.info(f"获取基金详情，基金ID: {fund_id}")
    
    # 查询基金
    fund = db.query(models.FundBasic).filter(models.FundBasic.id == fund_id).first()
    
    if not fund:
        raise HTTPException(status_code=404, detail="基金不存在")
    
    # 转换为响应格式
    return {
        "id": fund.id,
        "fund_code": fund.fund_code,
        "short_name": fund.short_name,
        "fund_name": fund.fund_name,
        "fund_type": fund.fund_type,
        "pinyin": fund.pinyin,
        "manager": fund.manager,
        "company": fund.company,
        "establish_date": fund.establish_date.isoformat() if fund.establish_date else None,
        "latest_nav": fund.latest_nav,
        "latest_nav_date": fund.latest_nav_date.isoformat() if fund.latest_nav_date else None,
        "created_at": fund.created_at.isoformat(),
        "updated_at": fund.updated_at.isoformat() if fund.updated_at else None
    }

@router.post("/import")
async def import_funds(
    source: str = Query(..., description="数据源，如eastmoney"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """导入基金列表"""
    logger.info(f"导入基金列表请求，数据源: {source}")
    
    try:
        # 转换数据源为枚举
        source_enum = DataSource(source)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 导入基金列表
        result = scrape_service.import_fund_list(source_enum)
        
        return {
            "status": "success",
            "message": "基金列表导入完成",
            "data": result
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"导入基金列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导入基金列表失败: {str(e)}")
