from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import desc, asc, or_, and_
from typing import List, Optional
from loguru import logger

# 导入数据库模型和依赖
from db import get_db
from db.models import FundCompany, FundBasic, FundRank, FundGrowth

router = APIRouter()


@router.get("/")
async def query():
    """健康检查接口"""
    return {"status": "query", "service": "查询接口", "message": "服务运行正常"}


# ---------------------- 基金公司查询接口 ----------------------
@router.get("/fund/company")
async def get_fund_companies(
    # 分页参数
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    # 过滤参数
    company_name: Optional[str] = Query(None, description="基金公司名称，支持模糊查询"),
    company_code: Optional[str] = Query(None, description="基金公司代码"),
    # 排序参数
    sort_by: Optional[str] = Query(
        None, description="排序字段，如 establish_date, created_at"
    ),
    sort_order: str = Query("asc", description="排序方式，asc 或 desc"),
    db: Session = Depends(get_db),
):
    """
    查询基金公司列表，支持分页、模糊查询和排序
    """
    try:
        query = db.query(FundCompany)

        # 应用过滤条件
        if company_name:
            query = query.filter(FundCompany.company_name.ilike(f"%{company_name}%"))
        if company_code:
            query = query.filter(FundCompany.company_code == company_code)

        # 应用排序
        if sort_by:
            if hasattr(FundCompany, sort_by):
                order_func = (
                    desc(getattr(FundCompany, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundCompany, sort_by))
                )
                query = query.order_by(order_func)
            else:
                raise HTTPException(
                    status_code=400, detail=f"排序字段 {sort_by} 不存在"
                )
        else:
            # 默认按创建时间排序
            query = query.order_by(desc(FundCompany.created_at))

        # 计算总数
        total = query.count()

        # 应用分页
        offset = (page - 1) * page_size
        companies = query.offset(offset).limit(page_size).all()

        # 构建响应
        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "data": [
                {
                    "id": company.id,
                    "company_code": company.company_code,
                    "company_name": company.company_name,
                    "short_name": company.short_name,
                    "establish_date": company.establish_date,
                    "registered_capital": company.registered_capital,
                    "address": company.address,
                    "contact_phone": company.contact_phone,
                    "website": company.website,
                    "description": company.description,
                    "created_at": company.created_at,
                    "updated_at": company.updated_at,
                }
                for company in companies
            ],
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询基金公司失败: {str(e)}")
        raise HTTPException(status_code=500, detail="查询基金公司失败")


@router.get("/fund/company/{company_id}")
async def get_fund_company_detail(company_id: int, db: Session = Depends(get_db)):
    """
    查询基金公司详情
    """
    try:
        company = db.query(FundCompany).filter(FundCompany.id == company_id).first()
        if not company:
            raise HTTPException(status_code=404, detail="基金公司不存在")

        # 获取该公司旗下的基金数量
        fund_count = (
            db.query(FundBasic).filter(FundBasic.company_id == company_id).count()
        )

        result = {
            "id": company.id,
            "company_code": company.company_code,
            "company_name": company.company_name,
            "short_name": company.short_name,
            "establish_date": company.establish_date,
            "registered_capital": company.registered_capital,
            "address": company.address,
            "contact_phone": company.contact_phone,
            "website": company.website,
            "description": company.description,
            "created_at": company.created_at,
            "updated_at": company.updated_at,
            "fund_count": fund_count,
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询基金公司详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail="查询基金公司详情失败")


# ---------------------- 基金基本信息查询接口 ----------------------
@router.get("/fund/basic")
async def get_fund_basic(
    # 分页参数
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    # 过滤参数
    fund_code: Optional[str] = Query(None, description="基金代码"),
    fund_name: Optional[str] = Query(None, description="基金名称，支持模糊查询"),
    fund_type: Optional[int] = Query(None, description="基金类型"),
    company_id: Optional[int] = Query(None, description="基金公司ID"),
    company_name: Optional[str] = Query(None, description="基金公司名称，支持模糊查询"),
    is_purchaseable: Optional[bool] = Query(None, description="是否可购买"),
    # 排序参数
    sort_by: Optional[str] = Query(
        None, description="排序字段，如 latest_nav, created_at"
    ),
    sort_order: str = Query("asc", description="排序方式，asc 或 desc"),
    db: Session = Depends(get_db),
):
    """
    查询基金基本信息列表，支持分页、多条件过滤和排序
    """
    try:
        query = db.query(FundBasic)

        # 应用过滤条件
        if fund_code:
            query = query.filter(FundBasic.fund_code == fund_code)
        if fund_name:
            query = query.filter(FundBasic.fund_name.ilike(f"%{fund_name}%"))
        if fund_type:
            query = query.filter(FundBasic.fund_type == fund_type)
        if company_id:
            query = query.filter(FundBasic.company_id == company_id)
        if company_name:
            query = query.filter(FundBasic.company_name.ilike(f"%{company_name}%"))
        if is_purchaseable is not None:
            query = query.filter(FundBasic.is_purchaseable == is_purchaseable)

        # 应用排序
        if sort_by:
            if hasattr(FundBasic, sort_by):
                order_func = (
                    desc(getattr(FundBasic, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundBasic, sort_by))
                )
                query = query.order_by(order_func)
            else:
                raise HTTPException(
                    status_code=400, detail=f"排序字段 {sort_by} 不存在"
                )
        else:
            # 默认按创建时间排序
            query = query.order_by(desc(FundBasic.created_at))

        # 计算总数
        total = query.count()

        # 应用分页
        offset = (page - 1) * page_size
        funds = query.offset(offset).limit(page_size).all()

        # 构建响应
        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "data": [
                {
                    "id": fund.id,
                    "fund_code": fund.fund_code,
                    "short_name": fund.short_name,
                    "fund_name": fund.fund_name,
                    "fund_type": fund.fund_type,
                    "pinyin": fund.pinyin,
                    "manager": fund.manager,
                    "company_id": fund.company_id,
                    "company_name": fund.company_name,
                    "launch_date": fund.launch_date,
                    "latest_nav": fund.latest_nav,
                    "latest_nav_date": fund.latest_nav_date,
                    "is_purchaseable": fund.is_purchaseable,
                    "purchase_start_date": fund.purchase_start_date,
                    "purchase_end_date": fund.purchase_end_date,
                    "purchase_min_amount": fund.purchase_min_amount,
                    "redemption_min_amount": fund.redemption_min_amount,
                    "risk_level": fund.risk_level,
                    "purchase_fee": fund.purchase_fee,
                    "redemption_fee": fund.redemption_fee,
                    "purchase_fee_rate": fund.purchase_fee_rate,
                    "created_at": fund.created_at,
                    "updated_at": fund.updated_at,
                }
                for fund in funds
            ],
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询基金基本信息失败: {str(e)}")
        raise HTTPException(status_code=500, detail="查询基金基本信息失败")


@router.get("/fund/basic/{fund_id}")
async def get_fund_basic_detail(fund_id: int, db: Session = Depends(get_db)):
    """
    查询基金基本信息详情，包含公司信息
    """
    try:
        # 使用join查询获取基金基本信息和公司信息
        fund = db.query(FundBasic).filter(FundBasic.id == fund_id).first()
        if not fund:
            raise HTTPException(status_code=404, detail="基金不存在")

        # 构建响应，包含公司信息
        result = {
            "id": fund.id,
            "fund_code": fund.fund_code,
            "short_name": fund.short_name,
            "fund_name": fund.fund_name,
            "fund_type": fund.fund_type,
            "pinyin": fund.pinyin,
            "manager": fund.manager,
            "company": (
                {
                    "id": fund.company.id if fund.company else None,
                    "company_code": fund.company.company_code if fund.company else None,
                    "company_name": fund.company.company_name if fund.company else None,
                }
                if fund.company
                else None
            ),
            "launch_date": fund.launch_date,
            "latest_nav": fund.latest_nav,
            "latest_nav_date": fund.latest_nav_date,
            "is_purchaseable": fund.is_purchaseable,
            "purchase_start_date": fund.purchase_start_date,
            "purchase_end_date": fund.purchase_end_date,
            "purchase_min_amount": fund.purchase_min_amount,
            "redemption_min_amount": fund.redemption_min_amount,
            "risk_level": fund.risk_level,
            "purchase_fee": fund.purchase_fee,
            "redemption_fee": fund.redemption_fee,
            "purchase_fee_rate": fund.purchase_fee_rate,
            "created_at": fund.created_at,
            "updated_at": fund.updated_at,
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询基金基本信息详情失败: {str(e)}")
        raise HTTPException(status_code=500, detail="查询基金基本信息详情失败")


# ---------------------- 基金排行查询接口 ----------------------
@router.get("/fund/rank")
async def get_fund_rank(
    # 分页参数
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    # 过滤参数
    fund_code: Optional[str] = Query(None, description="基金代码"),
    fund_name: Optional[str] = Query(None, description="基金名称，支持模糊查询"),
    rank_type: Optional[str] = Query(None, description="排行类型"),
    min_nav: Optional[float] = Query(None, description="最小单位净值"),
    max_nav: Optional[float] = Query(None, description="最大单位净值"),
    # 排序参数
    sort_by: Optional[str] = Query(
        "rank", description="排序字段，如 rank, daily_growth, weekly_growth"
    ),
    sort_order: str = Query("asc", description="排序方式，asc 或 desc"),
    db: Session = Depends(get_db),
):
    """
    查询基金排行列表，支持分页、多条件过滤和排序
    """
    try:
        # 使用join查询基金排行和基金基本信息
        query = db.query(FundRank, FundBasic).join(
            FundBasic, FundRank.fund_id == FundBasic.id
        )

        # 应用过滤条件
        if fund_code:
            query = query.filter(FundBasic.fund_code == fund_code)
        if fund_name:
            query = query.filter(FundBasic.fund_name.ilike(f"%{fund_name}%"))
        if rank_type:
            query = query.filter(FundRank.rank_type == rank_type)
        if min_nav is not None:
            query = query.filter(FundRank.nav >= min_nav)
        if max_nav is not None:
            query = query.filter(FundRank.nav <= max_nav)

        # 应用排序
        if sort_by:
            if hasattr(FundRank, sort_by):
                order_func = (
                    desc(getattr(FundRank, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundRank, sort_by))
                )
                query = query.order_by(order_func)
            elif hasattr(FundBasic, sort_by):
                order_func = (
                    desc(getattr(FundBasic, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundBasic, sort_by))
                )
                query = query.order_by(order_func)
            else:
                raise HTTPException(
                    status_code=400, detail=f"排序字段 {sort_by} 不存在"
                )
        else:
            # 默认按排名排序
            query = query.order_by(asc(FundRank.rank))

        # 计算总数
        total = query.count()

        # 应用分页
        offset = (page - 1) * page_size
        rank_data = query.offset(offset).limit(page_size).all()

        # 构建响应
        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "data": [
                {
                    "id": rank.id,
                    "fund": {
                        "id": fund.id,
                        "fund_code": fund.fund_code,
                        "fund_name": fund.fund_name,
                        "short_name": fund.short_name,
                    },
                    "rank_date": rank.rank_date,
                    "rank": rank.rank,
                    "rank_type": rank.rank_type,
                    "nav": rank.nav,
                    "accum_nav": rank.accum_nav,
                    "daily_growth": rank.daily_growth,
                    "weekly_growth": rank.weekly_growth,
                    "monthly_growth": rank.monthly_growth,
                    "quarterly_growth": rank.quarterly_growth,
                    "yearly_growth": rank.yearly_growth,
                    "two_year_growth": rank.two_year_growth,
                    "three_year_growth": rank.three_year_growth,
                    "five_year_growth": rank.five_year_growth,
                    "ytd_growth": rank.ytd_growth,
                    "since_launch_growth": rank.since_launch_growth,
                    "created_at": rank.created_at,
                    "updated_at": rank.updated_at,
                }
                for rank, fund in rank_data
            ],
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询基金排行失败: {str(e)}")
        raise HTTPException(status_code=500, detail="查询基金排行失败")


# ---------------------- 基金增长率查询接口 ----------------------
@router.get("/fund/growth")
async def get_fund_growth(
    # 分页参数
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    # 过滤参数
    fund_code: Optional[str] = Query(None, description="基金代码"),
    fund_name: Optional[str] = Query(None, description="基金名称，支持模糊查询"),
    min_daily_growth: Optional[float] = Query(None, description="最小近1日涨幅"),
    max_daily_growth: Optional[float] = Query(None, description="最大近1日涨幅"),
    min_yearly_growth: Optional[float] = Query(None, description="最小近1年涨幅"),
    max_yearly_growth: Optional[float] = Query(None, description="最大近1年涨幅"),
    # 排序参数
    sort_by: Optional[str] = Query(
        "daily_growth", description="排序字段，如 daily_growth, yearly_growth"
    ),
    sort_order: str = Query("desc", description="排序方式，asc 或 desc"),
    db: Session = Depends(get_db),
):
    """
    查询基金增长率数据，支持分页、多条件过滤和排序
    """
    try:
        # 使用join查询基金增长率和基金基本信息
        query = db.query(FundGrowth, FundBasic).join(
            FundBasic, FundGrowth.fund_id == FundBasic.id
        )

        # 应用过滤条件
        if fund_code:
            query = query.filter(FundBasic.fund_code == fund_code)
        if fund_name:
            query = query.filter(FundBasic.fund_name.ilike(f"%{fund_name}%"))
        if min_daily_growth is not None:
            query = query.filter(FundGrowth.daily_growth >= min_daily_growth)
        if max_daily_growth is not None:
            query = query.filter(FundGrowth.daily_growth <= max_daily_growth)
        if min_yearly_growth is not None:
            query = query.filter(FundGrowth.yearly_growth >= min_yearly_growth)
        if max_yearly_growth is not None:
            query = query.filter(FundGrowth.yearly_growth <= max_yearly_growth)

        # 应用排序
        if sort_by:
            if hasattr(FundGrowth, sort_by):
                order_func = (
                    desc(getattr(FundGrowth, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundGrowth, sort_by))
                )
                query = query.order_by(order_func)
            elif hasattr(FundBasic, sort_by):
                order_func = (
                    desc(getattr(FundBasic, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundBasic, sort_by))
                )
                query = query.order_by(order_func)
            else:
                raise HTTPException(
                    status_code=400, detail=f"排序字段 {sort_by} 不存在"
                )
        else:
            # 默认按近1日涨幅降序排序
            query = query.order_by(desc(FundGrowth.daily_growth))

        # 计算总数
        total = query.count()

        # 应用分页
        offset = (page - 1) * page_size
        growth_data = query.offset(offset).limit(page_size).all()

        # 构建响应
        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "data": [
                {
                    "id": growth.id,
                    "fund": {
                        "id": fund.id,
                        "fund_code": fund.fund_code,
                        "fund_name": fund.fund_name,
                        "short_name": fund.short_name,
                    },
                    "daily_growth": growth.daily_growth,
                    "weekly_growth": growth.weekly_growth,
                    "monthly_growth": growth.monthly_growth,
                    "quarterly_growth": growth.quarterly_growth,
                    "yearly_growth": growth.yearly_growth,
                    "update_date": growth.update_date,
                    "created_at": growth.created_at,
                    "updated_at": growth.updated_at,
                }
                for growth, fund in growth_data
            ],
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"查询基金增长率失败: {str(e)}")
        raise HTTPException(status_code=500, detail="查询基金增长率失败")


# ---------------------- 组合查询接口 ----------------------
@router.get("/fund/combined")
async def get_combined_fund_data(
    # 分页参数
    page: int = Query(1, ge=1, description="页码"),
    page_size: int = Query(10, ge=1, le=100, description="每页条数"),
    # 过滤参数
    fund_name: Optional[str] = Query(None, description="基金名称，支持模糊查询"),
    company_name: Optional[str] = Query(None, description="基金公司名称，支持模糊查询"),
    min_yearly_growth: Optional[float] = Query(None, description="最小近1年涨幅"),
    min_monthly_growth: Optional[float] = Query(None, description="最小近1月涨幅"),
    max_risk_level: Optional[float] = Query(None, description="最大风险等级"),
    is_purchaseable: Optional[bool] = Query(True, description="是否可购买"),
    # 排序参数
    sort_by: Optional[str] = Query("yearly_growth", description="排序字段"),
    sort_order: str = Query("desc", description="排序方式，asc 或 desc"),
    db: Session = Depends(get_db),
):
    """
    组合查询基金数据，包含基本信息、排行数据和公司信息
    """
    try:
        # 复杂查询：join基金基本信息、排行数据和公司信息
        # 使用fund_rank表替代fund_growth表，因为rank表中已经包含了增长率数据
        query = (
            db.query(FundBasic, FundRank, FundCompany)
            .join(FundRank, FundBasic.id == FundRank.fund_id)
            .join(FundCompany, FundBasic.company_id == FundCompany.id)
        )

        # 应用过滤条件
        filters = []
        if fund_name:
            filters.append(FundBasic.fund_name.ilike(f"%{fund_name}%"))
        if company_name:
            filters.append(FundCompany.company_name.ilike(f"%{company_name}%"))
        if min_yearly_growth is not None:
            filters.append(FundRank.yearly_growth >= min_yearly_growth)
        if min_monthly_growth is not None:
            filters.append(FundRank.monthly_growth >= min_monthly_growth)
        if max_risk_level is not None:
            filters.append(FundBasic.risk_level <= max_risk_level)
        if is_purchaseable is not None:
            filters.append(FundBasic.is_purchaseable == is_purchaseable)

        if filters:
            query = query.filter(and_(*filters))

        # 应用排序
        if sort_by:
            if hasattr(FundBasic, sort_by):
                order_func = (
                    desc(getattr(FundBasic, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundBasic, sort_by))
                )
                query = query.order_by(order_func)
            elif hasattr(FundRank, sort_by):
                order_func = (
                    desc(getattr(FundRank, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundRank, sort_by))
                )
                query = query.order_by(order_func)
            elif hasattr(FundCompany, sort_by):
                order_func = (
                    desc(getattr(FundCompany, sort_by))
                    if sort_order == "desc"
                    else asc(getattr(FundCompany, sort_by))
                )
                query = query.order_by(order_func)
            else:
                raise HTTPException(
                    status_code=400, detail=f"排序字段 {sort_by} 不存在"
                )
        else:
            # 默认按近1年涨幅降序排序
            query = query.order_by(desc(FundRank.yearly_growth))

        # 计算总数
        total = query.count()

        # 应用分页
        offset = (page - 1) * page_size
        combined_data = query.offset(offset).limit(page_size).all()

        # 构建响应
        result = {
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size,
            "data": [
                {
                    "fund": {
                        "id": fund.id,
                        "fund_code": fund.fund_code,
                        "fund_name": fund.fund_name,
                        "short_name": fund.short_name,
                        "fund_type": fund.fund_type,
                        "manager": fund.manager,
                        "latest_nav": fund.latest_nav,
                        "risk_level": fund.risk_level,
                        "is_purchaseable": fund.is_purchaseable,
                    },
                    "rank": {
                        "rank": rank.rank,
                        "rank_date": rank.rank_date,
                        "nav": rank.nav,
                        "accum_nav": rank.accum_nav,
                        "daily_growth": rank.daily_growth,
                        "weekly_growth": rank.weekly_growth,
                        "monthly_growth": rank.monthly_growth,
                        "quarterly_growth": rank.quarterly_growth,
                        "yearly_growth": rank.yearly_growth,
                        "two_year_growth": rank.two_year_growth,
                        "three_year_growth": rank.three_year_growth,
                        "ytd_growth": rank.ytd_growth,
                    },
                    "company": {
                        "id": company.id,
                        "company_code": company.company_code,
                        "company_name": company.company_name,
                        "short_name": company.short_name,
                        "establish_date": company.establish_date,
                    },
                }
                for fund, rank, company in combined_data
            ],
        }

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"组合查询基金数据失败: {str(e)}")
        raise HTTPException(status_code=500, detail="组合查询基金数据失败")
