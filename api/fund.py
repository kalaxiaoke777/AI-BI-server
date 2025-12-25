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
    """导入基金列表（仅初始化使用，不覆盖已有数据）
    
    该接口用于初始化基金基础数据，首次调用后，后续调用将不再重复导入已存在的基金。
    基金基础数据（fund_basic表）将被固定，不会被覆盖或反复刷入。
    """
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

@router.post("/company/import")
async def import_fund_companies(
    source: str = Query(..., description="数据源，如eastmoney"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """导入基金公司列表（仅初始化使用，不覆盖已有数据）
    
    该接口用于初始化基金公司数据，首次调用后，后续调用将不再重复导入已存在的公司。
    基金公司数据将被固定，不会被覆盖或反复刷入。
    """
    logger.info(f"导入基金公司列表请求，数据源: {source}")
    
    try:
        # 转换数据源为枚举
        source_enum = DataSource(source)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 导入基金公司列表
        result = scrape_service.import_fund_company_list(source_enum)
        
        return {
            "status": "success",
            "message": "基金公司列表导入完成",
            "data": result
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"导入基金公司列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导入基金公司列表失败: {str(e)}")

@router.post("/rank/import")
async def import_fund_rank(
    source: str = Query(..., description="数据源，如eastmoney"),
    max_pages: Optional[int] = Query(None, description="最大页码，为None时获取所有数据，默认为None"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """导入基金排行数据（仅初始化使用）
    
    该接口用于初始化基金排行数据，将爬取的数据存入数据库，并与fund_basic表关联。
    """
    logger.info(f"导入基金排行数据请求，数据源: {source}，最大页码: {max_pages}")
    
    try:
        # 转换数据源为枚举
        source_enum = DataSource(source)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 更新基金排行数据（初始化和更新使用相同的方法）
        result = scrape_service.update_fund_rank(source_enum, max_pages)
        
        return {
            "status": "success",
            "message": "基金排行数据导入完成",
            "data": result
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"导入基金排行数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"导入基金排行数据失败: {str(e)}")

@router.post("/growth/update")
async def update_fund_growth(
    source: str = Query(..., description="数据源，如eastmoney"),
    fund_code_list: Optional[List[str]] = Query(None, description="基金代码列表，为空则更新所有基金"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """更新基金历史涨幅数据"""
    logger.info(f"更新基金历史涨幅数据请求，数据源: {source}，基金数量: {len(fund_code_list) if fund_code_list else '所有'}")
    
    try:
        # 转换数据源为枚举
        source_enum = DataSource(source)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 更新基金历史涨幅数据
        result = scrape_service.update_fund_growth(source_enum, fund_code_list)
        
        return {
            "status": "success",
            "message": "基金历史涨幅数据更新完成",
            "data": result
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新基金历史涨幅数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新基金历史涨幅数据失败: {str(e)}")

@router.post("/rank/update")
async def update_fund_rank(
    source: str = Query(..., description="数据源，如eastmoney"),
    max_pages: Optional[int] = Query(None, description="最大页码，为None时获取所有数据，默认为None"),
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """更新基金排行数据"""
    logger.info(f"更新基金排行数据请求，数据源: {source}，最大页码: {max_pages}")
    
    try:
        # 转换数据源为枚举
        source_enum = DataSource(source)
        
        # 创建采集服务
        scrape_service = ScrapeService(db)
        
        # 更新基金排行数据
        result = scrape_service.update_fund_rank(source_enum, max_pages)
        
        return {
            "status": "success",
            "message": "基金排行数据更新完成",
            "data": result
        }
    
    except ValueError as e:
        logger.error(f"参数错误: {str(e)}")
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"更新基金排行数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"更新基金排行数据失败: {str(e)}")

@router.get("/{fund_id}/growth")
async def get_fund_growth(
    fund_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取基金历史涨幅数据"""
    logger.info(f"获取基金历史涨幅数据请求，基金ID: {fund_id}")
    
    try:
        # 查找基金
        fund = db.query(models.FundBasic).filter(models.FundBasic.id == fund_id).first()
        if not fund:
            raise HTTPException(status_code=404, detail="基金不存在")
        
        # 构建查询
        query = db.query(models.FundGrowth).filter(models.FundGrowth.fund_id == fund_id)
        
        # 按更新日期降序排序
        growth_data = query.order_by(models.FundGrowth.update_date.desc()).all()
        
        # 转换为响应格式
        result = []
        for growth in growth_data:
            result.append({
                "id": growth.id,
                "fund_id": growth.fund_id,
                "daily_growth": growth.daily_growth,
                "weekly_growth": growth.weekly_growth,
                "monthly_growth": growth.monthly_growth,
                "quarterly_growth": growth.quarterly_growth,
                "yearly_growth": growth.yearly_growth,
                "update_date": growth.update_date.isoformat() if growth.update_date else None,
                "created_at": growth.created_at.isoformat(),
                "updated_at": growth.updated_at.isoformat() if growth.updated_at else None
            })
        
        return {
            "status": "success",
            "data": {
                "fund_id": fund_id,
                "fund_code": fund.fund_code,
                "fund_name": fund.fund_name,
                "growth_data": result
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取基金历史涨幅数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取基金历史涨幅数据失败: {str(e)}")

@router.get("/companies")
async def get_fund_companies(
    page: int = 1,
    page_size: int = 10,
    company_name: Optional[str] = None,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取基金公司列表"""
    logger.info(f"获取基金公司列表请求，页码: {page}，每页大小: {page_size}，公司名称: {company_name}")
    
    try:
        # 构建查询
        query = db.query(models.FundCompany)
        
        # 应用过滤条件
        if company_name:
            query = query.filter(models.FundCompany.company_name.like(f"%{company_name}%"))
        
        # 计算总数
        total = query.count()
        
        # 分页查询
        companies = query.offset((page - 1) * page_size).limit(page_size).all()
        
        # 转换为响应格式
        result = []
        for company in companies:
            result.append({
                "id": company.id,
                "company_code": company.company_code,
                "company_name": company.company_name,
                "short_name": company.short_name,
                "establish_date": company.establish_date.isoformat() if company.establish_date else None,
                "registered_capital": company.registered_capital,
                "address": company.address,
                "contact_phone": company.contact_phone,
                "website": company.website,
                "created_at": company.created_at.isoformat(),
                "updated_at": company.updated_at.isoformat() if company.updated_at else None
            })
        
        return {
            "total": total,
            "page": page,
            "page_size": page_size,
            "data": result
        }
    
    except Exception as e:
        logger.error(f"获取基金公司列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取基金公司列表失败: {str(e)}")

@router.get("/companies/{company_id}")
async def get_fund_company_detail(
    company_id: int,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取基金公司详情"""
    logger.info(f"获取基金公司详情请求，公司ID: {company_id}")
    
    try:
        # 查找公司
        company = db.query(models.FundCompany).filter(models.FundCompany.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="基金公司不存在")
        
        # 转换为响应格式
        return {
            "status": "success",
            "data": {
                "id": company.id,
                "company_code": company.company_code,
                "company_name": company.company_name,
                "short_name": company.short_name,
                "establish_date": company.establish_date.isoformat() if company.establish_date else None,
                "registered_capital": company.registered_capital,
                "address": company.address,
                "contact_phone": company.contact_phone,
                "website": company.website,
                "description": company.description,
                "created_at": company.created_at.isoformat(),
                "updated_at": company.updated_at.isoformat() if company.updated_at else None
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取基金公司详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取基金公司详情失败: {str(e)}")

@router.get("/companies/{company_id}/funds")
async def get_company_funds(
    company_id: int,
    page: int = 1,
    page_size: int = 10,
    db: Session = Depends(get_db)
) -> Dict[str, Any]:
    """获取基金公司发行的基金列表"""
    logger.info(f"获取基金公司发行的基金列表请求，公司ID: {company_id}，页码: {page}，每页大小: {page_size}")
    
    try:
        # 查找公司
        company = db.query(models.FundCompany).filter(models.FundCompany.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="基金公司不存在")
        
        # 构建查询
        query = db.query(models.FundBasic).filter(models.FundBasic.company_id == company_id)
        
        # 计算总数
        total = query.count()
        
        # 分页查询
        funds = query.offset((page - 1) * page_size).limit(page_size).all()
        
        # 转换为响应格式
        result = []
        for fund in funds:
            result.append({
                "id": fund.id,
                "fund_code": fund.fund_code,
                "short_name": fund.short_name,
                "fund_name": fund.fund_name,
                "fund_type": fund.fund_type,
                "pinyin": fund.pinyin,
                "manager": fund.manager,
                "company_id": fund.company_id,
                "company_name": fund.company_name,
                "establish_date": fund.establish_date.isoformat() if fund.establish_date else None,
                "latest_nav": fund.latest_nav,
                "latest_nav_date": fund.latest_nav_date.isoformat() if fund.latest_nav_date else None,
                "is_purchaseable": fund.is_purchaseable,
                "purchase_min_amount": fund.purchase_min_amount,
                "redemption_min_amount": fund.redemption_min_amount,
                "risk_level": fund.risk_level,
                "created_at": fund.created_at.isoformat(),
                "updated_at": fund.updated_at.isoformat() if fund.updated_at else None
            })
        
        return {
            "status": "success",
            "data": {
                "company_id": company_id,
                "company_name": company.company_name,
                "total": total,
                "page": page,
                "page_size": page_size,
                "funds": result
            }
        }
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"获取基金公司发行的基金列表失败: {str(e)}")
        raise HTTPException(status_code=500, detail=f"获取基金公司发行的基金列表失败: {str(e)}")
