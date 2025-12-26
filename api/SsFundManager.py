from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from typing import List, Dict, Any, Optional
from loguru import logger
from pydantic import BaseModel, Field
from datetime import datetime

from db import get_db
from db.models import (
    User, UserFavoriteFund, UserFundHolding, FundTransaction,
    FundBasic, TransactionType
)
from api.userManager import get_current_user

# 创建路由
router = APIRouter()

# 模型定义
class FavoriteFundRequest(BaseModel):
    fund_id: Optional[int] = Field(None, description="基金ID")
    fund_code: Optional[str] = Field(None, description="基金代码")

class FundPurchaseRequest(BaseModel):
    fund_id: Optional[int] = Field(None, description="基金ID")
    fund_code: Optional[str] = Field(None, description="基金代码")
    amount: float = Field(..., gt=0, description="购买金额")

class FundRedeemRequest(BaseModel):
    holding_id: int = Field(..., description="持有记录ID")
    shares: float = Field(..., gt=0, description="赎回份额")

class FavoriteFundResponse(BaseModel):
    id: int
    user_id: int
    fund_id: int
    fund_code: str
    fund_name: str
    created_at: datetime
    
    class Config:
        from_attributes = True

class FundHoldingResponse(BaseModel):
    id: int
    user_id: int
    fund_id: int
    fund_code: str
    fund_name: str
    shares: float
    purchase_price: float
    current_price: float
    total_cost: float
    current_value: float
    daily_profit: float
    holding_profit: float
    holding_profit_rate: float
    is_holding: bool
    created_at: datetime
    updated_at: datetime
    
    class Config:
        from_attributes = True

class FundTransactionResponse(BaseModel):
    id: int
    user_id: int
    fund_id: int
    fund_code: str
    fund_name: str
    transaction_type: TransactionType
    shares: float
    transaction_price: float
    transaction_amount: float
    transaction_time: datetime
    status: str
    created_at: datetime
    
    class Config:
        from_attributes = True

class UserTotalProfitResponse(BaseModel):
    total_holding_value: float
    total_cost: float
    total_holding_profit: float
    total_holding_profit_rate: float
    total_daily_profit: float
    total_transaction_count: int
    total_holding_count: int

class FundInfoResponse(BaseModel):
    id: int
    fund_code: str
    fund_name: str
    latest_nav: Optional[float]
    latest_nav_date: Optional[datetime]
    fund_type: Optional[int]
    company_name: Optional[str]
    manager: Optional[str]
    
    class Config:
        from_attributes = True

# 工具函数
def get_fund_by_id_or_code(db: Session, fund_id: Optional[int] = None, fund_code: Optional[str] = None) -> Optional[FundBasic]:
    """根据基金ID或代码获取基金信息"""
    if fund_id:
        return db.query(FundBasic).filter(FundBasic.id == fund_id).first()
    elif fund_code:
        return db.query(FundBasic).filter(FundBasic.fund_code == fund_code).first()
    return None

def calculate_holding_profit(holding: UserFundHolding) -> None:
    """计算持有收益"""
    # 计算当前价值
    holding.current_value = holding.shares * holding.current_price
    # 计算持有收益
    holding.holding_profit = holding.current_value - holding.total_cost
    # 计算持有收益率
    if holding.total_cost > 0:
        holding.holding_profit_rate = (holding.holding_profit / holding.total_cost) * 100
    else:
        holding.holding_profit_rate = 0

# 路由
@router.post("/favorite-funds", response_model=FavoriteFundResponse, tags=["自选基金"])
async def add_favorite_fund(
    favorite_data: FavoriteFundRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """添加自选基金"""
    # 检查是否提供了基金ID或代码
    if not favorite_data.fund_id and not favorite_data.fund_code:
        raise HTTPException(status_code=400, detail="必须提供基金ID或基金代码")
    
    # 获取基金信息
    fund = get_fund_by_id_or_code(db, favorite_data.fund_id, favorite_data.fund_code)
    if not fund:
        raise HTTPException(status_code=404, detail="基金不存在")
    
    # 检查是否已添加到自选
    existing_favorite = db.query(UserFavoriteFund).filter(
        UserFavoriteFund.user_id == current_user.id,
        UserFavoriteFund.fund_id == fund.id
    ).first()
    
    if existing_favorite:
        raise HTTPException(status_code=400, detail="该基金已在自选列表中")
    
    # 添加到自选
    favorite_fund = UserFavoriteFund(
        user_id=current_user.id,
        fund_id=fund.id,
        fund_code=fund.fund_code
    )
    
    db.add(favorite_fund)
    db.commit()
    db.refresh(favorite_fund)
    
    logger.info(f"添加自选基金成功，用户ID: {current_user.id}, 基金代码: {fund.fund_code}")
    
    # 构造响应
    return {
        "id": favorite_fund.id,
        "user_id": favorite_fund.user_id,
        "fund_id": favorite_fund.fund_id,
        "fund_code": favorite_fund.fund_code,
        "fund_name": fund.fund_name,
        "created_at": favorite_fund.created_at
    }

@router.get("/favorite-funds", response_model=List[FavoriteFundResponse], tags=["自选基金"])
async def get_favorite_funds(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取自选基金列表"""
    favorite_funds = db.query(UserFavoriteFund).filter(
        UserFavoriteFund.user_id == current_user.id
    ).all()
    
    # 构造响应，包含基金名称
    result = []
    for favorite in favorite_funds:
        fund = db.query(FundBasic).filter(FundBasic.id == favorite.fund_id).first()
        if fund:
            result.append({
                "id": favorite.id,
                "user_id": favorite.user_id,
                "fund_id": favorite.fund_id,
                "fund_code": favorite.fund_code,
                "fund_name": fund.fund_name,
                "created_at": favorite.created_at
            })
    
    return result

@router.delete("/favorite-funds/{favorite_id}", tags=["自选基金"])
async def remove_favorite_fund(
    favorite_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """移除自选基金"""
    favorite_fund = db.query(UserFavoriteFund).filter(
        UserFavoriteFund.id == favorite_id,
        UserFavoriteFund.user_id == current_user.id
    ).first()
    
    if not favorite_fund:
        raise HTTPException(status_code=404, detail="自选基金不存在")
    
    db.delete(favorite_fund)
    db.commit()
    
    logger.info(f"移除自选基金成功，用户ID: {current_user.id}, 基金代码: {favorite_fund.fund_code}")
    
    return {"status": "success", "message": "自选基金已移除"}

@router.post("/holdings/purchase", response_model=FundHoldingResponse, tags=["基金持有"])
async def purchase_fund(
    purchase_data: FundPurchaseRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """购买基金"""
    # 检查是否提供了基金ID或代码
    if not purchase_data.fund_id and not purchase_data.fund_code:
        raise HTTPException(status_code=400, detail="必须提供基金ID或基金代码")
    
    # 获取基金信息
    fund = get_fund_by_id_or_code(db, purchase_data.fund_id, purchase_data.fund_code)
    if not fund:
        raise HTTPException(status_code=404, detail="基金不存在")
    
    # 检查基金是否有最新净值
    if not fund.latest_nav:
        raise HTTPException(status_code=400, detail="该基金暂无最新净值数据，无法购买")
    
    # 计算购买份额（简化计算，不考虑手续费）
    shares = purchase_data.amount / fund.latest_nav
    shares = round(shares, 4)  # 保留4位小数
    
    # 检查是否已持有该基金
    existing_holding = db.query(UserFundHolding).filter(
        UserFundHolding.user_id == current_user.id,
        UserFundHolding.fund_id == fund.id,
        UserFundHolding.is_holding == True
    ).first()
    
    if existing_holding:
        # 更新现有持有记录
        total_shares = existing_holding.shares + shares
        total_cost = existing_holding.total_cost + purchase_data.amount
        avg_purchase_price = total_cost / total_shares
        avg_purchase_price = round(avg_purchase_price, 4)
        
        existing_holding.shares = total_shares
        existing_holding.purchase_price = avg_purchase_price
        existing_holding.total_cost = total_cost
        existing_holding.current_price = fund.latest_nav
        
        # 重新计算收益
        calculate_holding_profit(existing_holding)
        
        db.commit()
        db.refresh(existing_holding)
        
        holding = existing_holding
    else:
        # 创建新的持有记录
        holding = UserFundHolding(
            user_id=current_user.id,
            fund_id=fund.id,
            fund_code=fund.fund_code,
            fund_name=fund.fund_name,
            shares=shares,
            purchase_price=fund.latest_nav,
            current_price=fund.latest_nav,
            total_cost=purchase_data.amount,
            current_value=purchase_data.amount,
            daily_profit=0,
            holding_profit=0,
            holding_profit_rate=0,
            is_holding=True
        )
        
        db.add(holding)
        db.commit()
        db.refresh(holding)
    
    # 记录交易
    transaction = FundTransaction(
        user_id=current_user.id,
        fund_id=fund.id,
        fund_code=fund.fund_code,
        fund_name=fund.fund_name,
        transaction_type=TransactionType.PURCHASE,
        shares=shares,
        transaction_price=fund.latest_nav,
        transaction_amount=purchase_data.amount,
        status="completed"
    )
    
    db.add(transaction)
    db.commit()
    
    logger.info(f"购买基金成功，用户ID: {current_user.id}, 基金代码: {fund.fund_code}, 金额: {purchase_data.amount}")
    
    return holding

@router.post("/holdings/redeem", response_model=FundTransactionResponse, tags=["基金持有"])
async def redeem_fund(
    redeem_data: FundRedeemRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """赎回基金"""
    # 获取持有记录
    holding = db.query(UserFundHolding).filter(
        UserFundHolding.id == redeem_data.holding_id,
        UserFundHolding.user_id == current_user.id,
        UserFundHolding.is_holding == True
    ).first()
    
    if not holding:
        raise HTTPException(status_code=404, detail="持有记录不存在")
    
    # 检查赎回份额是否超过持有份额
    if redeem_data.shares > holding.shares:
        raise HTTPException(status_code=400, detail="赎回份额不能超过持有份额")
    
    # 获取基金最新净值
    fund = db.query(FundBasic).filter(FundBasic.id == holding.fund_id).first()
    if not fund or not fund.latest_nav:
        raise HTTPException(status_code=400, detail="该基金暂无最新净值数据，无法赎回")
    
    # 计算赎回金额（简化计算，不考虑手续费）
    redeem_amount = redeem_data.shares * fund.latest_nav
    redeem_amount = round(redeem_amount, 2)
    
    # 更新持有记录
    if redeem_data.shares == holding.shares:
        # 全部赎回
        holding.is_holding = False
    else:
        # 部分赎回
        remaining_shares = holding.shares - redeem_data.shares
        remaining_cost = (holding.total_cost / holding.shares) * remaining_shares
        
        holding.shares = remaining_shares
        holding.total_cost = remaining_cost
        holding.current_price = fund.latest_nav
        
        # 重新计算收益
        calculate_holding_profit(holding)
    
    # 记录交易
    transaction = FundTransaction(
        user_id=current_user.id,
        fund_id=holding.fund_id,
        fund_code=holding.fund_code,
        fund_name=holding.fund_name,
        transaction_type=TransactionType.REDEEM,
        shares=redeem_data.shares,
        transaction_price=fund.latest_nav,
        transaction_amount=redeem_amount,
        status="completed"
    )
    
    db.add(transaction)
    db.commit()
    db.refresh(transaction)
    
    logger.info(f"赎回基金成功，用户ID: {current_user.id}, 基金代码: {holding.fund_code}, 份额: {redeem_data.shares}")
    
    return transaction

@router.get("/holdings", response_model=List[FundHoldingResponse], tags=["基金持有"])
async def get_fund_holdings(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取基金持有列表"""
    holdings = db.query(UserFundHolding).filter(
        UserFundHolding.user_id == current_user.id,
        UserFundHolding.is_holding == True
    ).all()
    
    # 更新最新净值和收益
    for holding in holdings:
        fund = db.query(FundBasic).filter(FundBasic.id == holding.fund_id).first()
        if fund and fund.latest_nav:
            holding.current_price = fund.latest_nav
            calculate_holding_profit(holding)
    
    db.commit()
    
    return holdings

@router.get("/transactions", response_model=List[FundTransactionResponse], tags=["交易记录"])
async def get_transaction_history(
    skip: int = Query(0, ge=0, description="跳过记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
    transaction_type: Optional[TransactionType] = Query(None, description="交易类型"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取交易记录"""
    query = db.query(FundTransaction).filter(
        FundTransaction.user_id == current_user.id
    )
    
    if transaction_type:
        query = query.filter(FundTransaction.transaction_type == transaction_type)
    
    transactions = query.order_by(FundTransaction.transaction_time.desc()).offset(skip).limit(limit).all()
    
    return transactions

@router.get("/total-profit", response_model=UserTotalProfitResponse, tags=["收益计算"])
async def get_total_profit(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取用户总收益"""
    # 获取所有持有基金
    holdings = db.query(UserFundHolding).filter(
        UserFundHolding.user_id == current_user.id,
        UserFundHolding.is_holding == True
    ).all()
    
    # 计算总收益
    total_holding_value = 0
    total_cost = 0
    total_holding_profit = 0
    total_daily_profit = 0
    
    for holding in holdings:
        # 更新最新净值
        fund = db.query(FundBasic).filter(FundBasic.id == holding.fund_id).first()
        if fund and fund.latest_nav:
            holding.current_price = fund.latest_nav
            calculate_holding_profit(holding)
        
        total_holding_value += holding.current_value
        total_cost += holding.total_cost
        total_holding_profit += holding.holding_profit
        total_daily_profit += holding.daily_profit
    
    db.commit()
    
    # 计算总持有收益率
    total_holding_profit_rate = 0
    if total_cost > 0:
        total_holding_profit_rate = (total_holding_profit / total_cost) * 100
    
    # 获取总交易次数
    total_transaction_count = db.query(FundTransaction).filter(
        FundTransaction.user_id == current_user.id
    ).count()
    
    return {
        "total_holding_value": round(total_holding_value, 2),
        "total_cost": round(total_cost, 2),
        "total_holding_profit": round(total_holding_profit, 2),
        "total_holding_profit_rate": round(total_holding_profit_rate, 2),
        "total_daily_profit": round(total_daily_profit, 2),
        "total_transaction_count": total_transaction_count,
        "total_holding_count": len(holdings)
    }

@router.get("/funds/{fund_id_or_code}", response_model=FundInfoResponse, tags=["基金信息"])
async def get_fund_info(
    fund_id_or_code: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db)
):
    """获取基金详细信息"""
    # 尝试解析为整数（基金ID）
    try:
        fund_id = int(fund_id_or_code)
        fund = db.query(FundBasic).filter(FundBasic.id == fund_id).first()
    except ValueError:
        # 作为基金代码处理
        fund = db.query(FundBasic).filter(FundBasic.fund_code == fund_id_or_code).first()
    
    if not fund:
        raise HTTPException(status_code=404, detail="基金不存在")
    
    return fund
