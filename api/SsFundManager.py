from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import List, Dict, Any, Optional
from loguru import logger
from pydantic import BaseModel, Field
from datetime import datetime, date, time, timedelta
import chinese_calendar

from db import get_db
from db.models import (
    User,
    UserFavoriteFund,
    UserFundHolding,
    FundTransaction,
    FundBasic,
    FundRank,
    FundDaily,
    PendingFundTransaction,
    TransactionType,
)
from api.userManager import get_current_user


# ==================== 交易规则常量 ====================
# 基金交易截止时间（15:00）
TRADING_CUTOFF_TIME = time(15, 0, 0)
# 交易状态
TRANSACTION_STATUS_PENDING = "pending"  # 待确认（T日15:00后或非交易日提交）
TRANSACTION_STATUS_CONFIRMED = "confirmed"  # 已确认（已按净值确认份额）
TRANSACTION_STATUS_COMPLETED = "completed"  # 已完成（资金已到账）


def is_trading_day(check_date: date = None) -> bool:
    """
    判断是否为交易日
    交易日：周一至周五且非法定节假日
    """
    if check_date is None:
        check_date = date.today()

    # 使用chinese_calendar库判断是否为工作日（已考虑调休）
    try:
        return chinese_calendar.is_workday(check_date)
    except Exception:
        # 如果库出错，则使用简单的周末判断
        return check_date.weekday() < 5


def get_next_trading_day(from_date: date = None) -> date:
    """
    获取下一个交易日
    """
    if from_date is None:
        from_date = date.today()

    next_day = from_date + timedelta(days=1)
    while not is_trading_day(next_day):
        next_day += timedelta(days=1)
    return next_day


def get_effective_trading_day(submit_time: datetime = None) -> tuple[date, str]:
    """
    获取有效交易日和交易状态

    规则：
    - 交易日15:00前提交：按当日净值确认，状态为confirmed
    - 交易日15:00后提交：按下一交易日净值确认，状态为pending
    - 非交易日提交：按下一交易日净值确认，状态为pending

    返回：(有效交易日, 交易状态)
    """
    if submit_time is None:
        submit_time = datetime.now()

    submit_date = submit_time.date()
    submit_time_only = submit_time.time()

    if is_trading_day(submit_date):
        if submit_time_only < TRADING_CUTOFF_TIME:
            # 交易日15:00前，按当日净值确认
            return submit_date, TRANSACTION_STATUS_CONFIRMED
        else:
            # 交易日15:00后，按下一交易日净值确认
            return get_next_trading_day(submit_date), TRANSACTION_STATUS_PENDING
    else:
        # 非交易日，按下一交易日净值确认
        return get_next_trading_day(submit_date), TRANSACTION_STATUS_PENDING


def get_trading_day_info(submit_time: datetime = None) -> dict:
    """
    获取交易日信息的详细说明
    """
    if submit_time is None:
        submit_time = datetime.now()

    effective_date, status = get_effective_trading_day(submit_time)
    today = submit_time.date()

    return {
        "submit_time": submit_time,
        "submit_date": today,
        "is_trading_day": is_trading_day(today),
        "is_before_cutoff": submit_time.time() < TRADING_CUTOFF_TIME,
        "effective_date": effective_date,
        "status": status,
        "cutoff_time": TRADING_CUTOFF_TIME.strftime("%H:%M"),
        "message": _get_trading_message(
            today, submit_time.time(), effective_date, status
        ),
    }


def _get_trading_message(
    today: date, current_time: time, effective_date: date, status: str
) -> str:
    """生成交易提示消息"""
    if not is_trading_day(today):
        return f"今日非交易日，您的交易将在下一个交易日({effective_date})按当日净值确认"
    elif current_time >= TRADING_CUTOFF_TIME:
        return f"已过15:00交易截止时间，您的交易将在下一个交易日({effective_date})按当日净值确认"
    else:
        return f"交易将按今日({effective_date})净值确认"


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


class PendingFundTransactionResponse(BaseModel):
    id: int
    user_id: int
    fund_id: int
    fund_code: str
    fund_name: str
    transaction_type: TransactionType
    amount: float
    submit_time: datetime
    effective_date: datetime
    can_cancel_until: datetime
    status: str
    note: Optional[str] = None
    created_at: datetime

    class Config:
        from_attributes = True


# 工具函数
def get_fund_by_id_or_code(
    db: Session, fund_id: Optional[int] = None, fund_code: Optional[str] = None
) -> Optional[FundBasic]:
    """根据基金ID或代码获取基金信息"""
    if fund_id:
        return db.query(FundBasic).filter(FundBasic.id == fund_id).first()
    elif fund_code:
        return db.query(FundBasic).filter(FundBasic.fund_code == fund_code).first()
    return None


def get_fund_daily_growth(db: Session, fund_id: int) -> Optional[float]:
    """
    获取基金当日涨幅
    优先从FundRank表获取，其次从FundDaily表获取
    """
    # 尝试从FundRank表获取最新涨幅
    latest_rank = (
        db.query(FundRank)
        .filter(FundRank.fund_id == fund_id)
        .order_by(desc(FundRank.rank_date))
        .first()
    )
    if latest_rank and latest_rank.daily_growth is not None:
        return latest_rank.daily_growth

    # 尝试从FundDaily表获取
    latest_daily = (
        db.query(FundDaily)
        .filter(FundDaily.fund_id == fund_id)
        .order_by(desc(FundDaily.trade_date))
        .first()
    )
    if latest_daily and latest_daily.daily_growth is not None:
        return latest_daily.daily_growth

    # 尝试从FundBasic表获取
    fund = db.query(FundBasic).filter(FundBasic.id == fund_id).first()
    if fund and fund.daily_growth is not None:
        return fund.daily_growth

    return None


def calculate_holding_profit(holding: UserFundHolding, db: Session = None) -> None:
    """
    计算持有收益（包括日收益）

    日收益计算公式：日收益 = 当前持仓价值 * 当日涨幅 / 100
    持有收益 = 当前价值 - 总成本
    """
    # 计算当前价值
    holding.current_value = holding.shares * holding.current_price
    # 计算持有收益
    holding.holding_profit = holding.current_value - holding.total_cost
    # 计算持有收益率
    if holding.total_cost > 0:
        holding.holding_profit_rate = (
            holding.holding_profit / holding.total_cost
        ) * 100
    else:
        holding.holding_profit_rate = 0

    # 计算日收益（根据当日涨幅）
    if db:
        daily_growth = get_fund_daily_growth(db, holding.fund_id)
        if daily_growth is not None:
            # 日收益 = 昨日市值 * 当日涨幅率 ≈ 当前市值 * 涨幅率 / (1 + 涨幅率)
            # 简化计算：日收益 = 当前市值 * 当日涨幅 / 100
            holding.daily_profit = round(holding.current_value * daily_growth / 100, 2)
        else:
            holding.daily_profit = 0
    else:
        holding.daily_profit = 0


def calculate_purchase_fee(amount: float, fee_rate_str: str) -> tuple[float, float]:
    """
    计算申购费用

    Args:
        amount: 申购金额
        fee_rate_str: 费率字符串（如 "0.15" 表示0.15%）

    Returns:
        (实际投资金额, 手续费)
    """
    try:
        fee_rate = float(fee_rate_str) / 100  # 转换为小数
    except (ValueError, TypeError):
        fee_rate = 0

    fee = round(amount * fee_rate, 2)
    actual_amount = amount - fee
    return actual_amount, fee


def calculate_redeem_fee(amount: float, fee_rate_str: str) -> tuple[float, float]:
    """
    计算赎回费用

    Args:
        amount: 赎回金额
        fee_rate_str: 费率字符串

    Returns:
        (实际到账金额, 手续费)
    """
    try:
        fee_rate = float(fee_rate_str) / 100
    except (ValueError, TypeError):
        fee_rate = 0

    fee = round(amount * fee_rate, 2)
    actual_amount = amount - fee
    return actual_amount, fee


def get_nav_by_date(db: Session, fund_id: int, target_date: date) -> Optional[float]:
    """
    根据日期获取基金当日净值（优先使用 FundDaily 表中的 trade_date）
    返回净值（nav）或 None
    """
    start = datetime.combine(target_date, time(0, 0))
    end = start + timedelta(days=1)
    record = (
        db.query(FundDaily)
        .filter(
            FundDaily.fund_id == fund_id,
            FundDaily.trade_date >= start,
            FundDaily.trade_date < end,
        )
        .order_by(desc(FundDaily.trade_date))
        .first()
    )
    if record and record.nav is not None:
        return record.nav
    # 尝试从 FundRank 中查找
    rank = (
        db.query(FundRank)
        .filter(
            FundRank.fund_id == fund_id,
            FundRank.rank_date >= start,
            FundRank.rank_date < end,
        )
        .order_by(desc(FundRank.rank_date))
        .first()
    )
    if rank and rank.nav is not None:
        return rank.nav
    return None


def process_pending_transactions(
    db: Session, process_date: date = None
) -> Dict[str, Any]:
    """
    处理到期的 PendingFundTransaction：
    - 在 process_date 的 15:00 之后运行（通常由定时任务触发）
    - 对于已到期且状态为 pending 的记录，尝试根据 effective_date 查找净值并确认交易
    返回处理统计信息
    """
    if process_date is None:
        process_date = date.today()

    results = {"processed": 0, "skipped_no_nav": 0, "errors": 0}

    # 选取应当处理的 pending 项（effective_date <= process_date 且 status == pending）
    pendings = (
        db.query(PendingFundTransaction)
        .filter(PendingFundTransaction.status == TRANSACTION_STATUS_PENDING)
        .all()
    )

    for p in pendings:
        try:
            eff_date = (
                p.effective_date.date()
                if isinstance(p.effective_date, datetime)
                else p.effective_date
            )
            if eff_date > process_date:
                continue

            # 查找确认日的净值
            nav = get_nav_by_date(db, p.fund_id, eff_date)
            if nav is None:
                results["skipped_no_nav"] += 1
                continue

            # 处理申购
            if p.transaction_type == TransactionType.PURCHASE:
                shares = round(p.amount / nav, 4)
                existing_holding = (
                    db.query(UserFundHolding)
                    .filter(
                        UserFundHolding.user_id == p.user_id,
                        UserFundHolding.fund_id == p.fund_id,
                        UserFundHolding.is_holding == True,
                    )
                    .first()
                )

                if existing_holding:
                    total_shares = existing_holding.shares + shares
                    total_cost = existing_holding.total_cost + p.amount
                    existing_holding.shares = total_shares
                    existing_holding.total_cost = total_cost
                    existing_holding.purchase_price = round(
                        total_cost / total_shares, 4
                    )
                    existing_holding.current_price = nav
                    calculate_holding_profit(existing_holding, db)
                    db.commit()
                else:
                    holding = UserFundHolding(
                        user_id=p.user_id,
                        fund_id=p.fund_id,
                        fund_code=p.fund_code,
                        fund_name=p.fund_name,
                        shares=shares,
                        purchase_price=nav,
                        current_price=nav,
                        total_cost=p.amount,
                        current_value=round(shares * nav, 2),
                        daily_profit=0,
                        holding_profit=0,
                        holding_profit_rate=0,
                        is_holding=True,
                    )
                    db.add(holding)
                    db.commit()

                # 创建交易记录（confirmed）
                tx = FundTransaction(
                    user_id=p.user_id,
                    fund_id=p.fund_id,
                    fund_code=p.fund_code,
                    fund_name=p.fund_name,
                    transaction_type=TransactionType.PURCHASE,
                    shares=shares,
                    transaction_price=nav,
                    transaction_amount=p.amount,
                    status=TRANSACTION_STATUS_CONFIRMED,
                )
                db.add(tx)

            else:
                # 处理赎回，p.amount 表示赎回份额
                shares = p.amount
                gross = round(shares * nav, 2)
                fee_rate = None
                fund = db.query(FundBasic).filter(FundBasic.id == p.fund_id).first()
                if fund:
                    fee_rate = fund.redemption_fee or "0"
                actual_amount, fee = calculate_redeem_fee(gross, fee_rate)

                # 更新持仓
                holding = (
                    db.query(UserFundHolding)
                    .filter(
                        UserFundHolding.user_id == p.user_id,
                        UserFundHolding.fund_id == p.fund_id,
                    )
                    .first()
                )
                if holding:
                    if abs(shares - holding.shares) < 0.0001:
                        holding.is_holding = False
                        holding.shares = 0
                        holding.current_value = 0
                    else:
                        remaining_shares = holding.shares - shares
                        remaining_cost = (
                            holding.total_cost / holding.shares
                        ) * remaining_shares
                        holding.shares = round(remaining_shares, 4)
                        holding.total_cost = round(remaining_cost, 2)
                        holding.current_price = nav
                        calculate_holding_profit(holding, db)
                    db.commit()

                tx = FundTransaction(
                    user_id=p.user_id,
                    fund_id=p.fund_id,
                    fund_code=p.fund_code,
                    fund_name=p.fund_name,
                    transaction_type=TransactionType.REDEEM,
                    shares=shares,
                    transaction_price=nav,
                    transaction_amount=round(actual_amount, 2),
                    status=TRANSACTION_STATUS_CONFIRMED,
                )
                db.add(tx)

            # 标记 pending 为已确认
            p.status = TRANSACTION_STATUS_CONFIRMED
            db.commit()
            results["processed"] += 1
        except Exception:
            db.rollback()
            results["errors"] += 1

    return results


# 路由
@router.post("/favorite-funds", response_model=FavoriteFundResponse, tags=["自选基金"])
async def add_favorite_fund(
    favorite_data: FavoriteFundRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
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
    existing_favorite = (
        db.query(UserFavoriteFund)
        .filter(
            UserFavoriteFund.user_id == current_user.id,
            UserFavoriteFund.fund_id == fund.id,
        )
        .first()
    )

    if existing_favorite:
        raise HTTPException(status_code=400, detail="该基金已在自选列表中")

    # 添加到自选
    favorite_fund = UserFavoriteFund(
        user_id=current_user.id, fund_id=fund.id, fund_code=fund.fund_code
    )

    db.add(favorite_fund)
    db.commit()
    db.refresh(favorite_fund)

    logger.info(
        f"添加自选基金成功，用户ID: {current_user.id}, 基金代码: {fund.fund_code}"
    )

    # 构造响应
    return {
        "id": favorite_fund.id,
        "user_id": favorite_fund.user_id,
        "fund_id": favorite_fund.fund_id,
        "fund_code": favorite_fund.fund_code,
        "fund_name": fund.fund_name,
        "created_at": favorite_fund.created_at,
    }


@router.get(
    "/favorite-funds", response_model=List[FavoriteFundResponse], tags=["自选基金"]
)
async def get_favorite_funds(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
):
    """获取自选基金列表"""
    favorite_funds = (
        db.query(UserFavoriteFund)
        .filter(UserFavoriteFund.user_id == current_user.id)
        .all()
    )

    # 构造响应，包含基金名称
    result = []
    for favorite in favorite_funds:
        fund = db.query(FundBasic).filter(FundBasic.id == favorite.fund_id).first()
        if fund:
            result.append(
                {
                    "id": favorite.id,
                    "user_id": favorite.user_id,
                    "fund_id": favorite.fund_id,
                    "fund_code": favorite.fund_code,
                    "fund_name": fund.fund_name,
                    "created_at": favorite.created_at,
                }
            )

    return result


@router.delete("/favorite-funds/{favorite_id}", tags=["自选基金"])
async def remove_favorite_fund(
    favorite_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """移除自选基金"""
    favorite_fund = (
        db.query(UserFavoriteFund)
        .filter(
            UserFavoriteFund.id == favorite_id,
            UserFavoriteFund.user_id == current_user.id,
        )
        .first()
    )

    if not favorite_fund:
        raise HTTPException(status_code=404, detail="自选基金不存在")

    db.delete(favorite_fund)
    db.commit()

    logger.info(
        f"移除自选基金成功，用户ID: {current_user.id}, 基金代码: {favorite_fund.fund_code}"
    )

    return {"status": "success", "message": "自选基金已移除"}


@router.post("/holdings/purchase", response_model=Dict[str, Any], tags=["基金持有"])
async def purchase_fund(
    purchase_data: FundPurchaseRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    购买基金

    交易规则：
    - 交易日15:00前提交：按当日净值确认
    - 交易日15:00后或非交易日提交：按下一个交易日净值确认
    - 申购费用会从申购金额中扣除
    """
    # 检查是否提供了基金ID或代码
    if not purchase_data.fund_id and not purchase_data.fund_code:
        raise HTTPException(status_code=400, detail="必须提供基金ID或基金代码")

    # 获取基金信息
    fund = get_fund_by_id_or_code(db, purchase_data.fund_id, purchase_data.fund_code)
    if not fund:
        raise HTTPException(status_code=404, detail="基金不存在")

    # 检查基金是否可购买
    if fund.is_purchaseable is False:
        raise HTTPException(status_code=400, detail="该基金暂停申购")

    # 检查基金是否有最新净值
    if not fund.latest_nav:
        raise HTTPException(status_code=400, detail="该基金暂无最新净值数据，无法购买")

    # 检查最低申购金额
    if fund.purchase_min_amount and purchase_data.amount < fund.purchase_min_amount:
        raise HTTPException(
            status_code=400,
            detail=f"申购金额不能低于最低申购金额 {fund.purchase_min_amount} 元",
        )

    # 获取交易日信息
    now = datetime.now()
    trading_info = get_trading_day_info(now)
    effective_date, transaction_status = get_effective_trading_day(now)

    # 计算申购费用
    fee_rate = fund.purchase_fee_rate or fund.purchase_fee or "0"
    actual_amount, purchase_fee = calculate_purchase_fee(purchase_data.amount, fee_rate)

    # 如果交易在当日15:00前并且是交易日，直接确认并写入持仓；否则写入等待表
    if transaction_status == TRANSACTION_STATUS_CONFIRMED:
        # 计算购买份额（使用扣除手续费后的金额）
        shares = round(actual_amount / fund.latest_nav, 4)

        # 检查是否已持有该基金
        existing_holding = (
            db.query(UserFundHolding)
            .filter(
                UserFundHolding.user_id == current_user.id,
                UserFundHolding.fund_id == fund.id,
                UserFundHolding.is_holding == True,
            )
            .first()
        )

        if existing_holding:
            total_shares = existing_holding.shares + shares
            total_cost = existing_holding.total_cost + actual_amount
            avg_purchase_price = round(total_cost / total_shares, 4)

            existing_holding.shares = total_shares
            existing_holding.purchase_price = avg_purchase_price
            existing_holding.total_cost = total_cost
            existing_holding.current_price = fund.latest_nav

            calculate_holding_profit(existing_holding, db)

            db.commit()
            db.refresh(existing_holding)

            holding = existing_holding
        else:
            holding = UserFundHolding(
                user_id=current_user.id,
                fund_id=fund.id,
                fund_code=fund.fund_code,
                fund_name=fund.fund_name,
                shares=shares,
                purchase_price=fund.latest_nav,
                current_price=fund.latest_nav,
                total_cost=actual_amount,
                current_value=actual_amount,
                daily_profit=0,
                holding_profit=0,
                holding_profit_rate=0,
                is_holding=True,
            )

            db.add(holding)
            db.commit()
            db.refresh(holding)

        # 记录已确认交易
        transaction = FundTransaction(
            user_id=current_user.id,
            fund_id=fund.id,
            fund_code=fund.fund_code,
            fund_name=fund.fund_name,
            transaction_type=TransactionType.PURCHASE,
            shares=shares,
            transaction_price=fund.latest_nav,
            transaction_amount=actual_amount,
            status=TRANSACTION_STATUS_CONFIRMED,
        )

        db.add(transaction)
        db.commit()

        logger.info(
            f"购买基金已确认，用户ID: {current_user.id}, 基金代码: {fund.fund_code}, 金额: {purchase_data.amount}, 手续费: {purchase_fee}"
        )

        return {
            "holding": holding,
            "transaction_info": {
                "申购金额": purchase_data.amount,
                "申购费用": purchase_fee,
                "实际投资金额": actual_amount,
                "确认份额": shares,
                "确认净值": fund.latest_nav,
                "交易状态": TRANSACTION_STATUS_CONFIRMED,
                "有效交易日": str(effective_date),
            },
            "trading_info": trading_info,
        }
    else:
        # 写入等待表，等待在 effective_date 当天 15:00 后系统批量确认
        can_cancel_until = datetime.combine(effective_date, TRADING_CUTOFF_TIME)
        pending = PendingFundTransaction(
            user_id=current_user.id,
            fund_id=fund.id,
            fund_code=fund.fund_code,
            fund_name=fund.fund_name,
            transaction_type=TransactionType.PURCHASE,
            amount=actual_amount,  # 存储实际扣费后的投资金额
            submit_time=now,
            effective_date=datetime.combine(effective_date, time(0, 0)),
            can_cancel_until=can_cancel_until,
            status=TRANSACTION_STATUS_PENDING,
            note=f"原始申购金额: {purchase_data.amount}, 申购费: {purchase_fee}",
        )

        db.add(pending)
        db.commit()
        db.refresh(pending)

        # 记录一条pending交易记录供查询
        transaction = FundTransaction(
            user_id=current_user.id,
            fund_id=fund.id,
            fund_code=fund.fund_code,
            fund_name=fund.fund_name,
            transaction_type=TransactionType.PURCHASE,
            shares=0,
            transaction_price=0,
            transaction_amount=purchase_data.amount,
            status=TRANSACTION_STATUS_PENDING,
        )

        db.add(transaction)
        db.commit()

        logger.info(
            f"购买基金已进入等待表，用户ID: {current_user.id}, 基金代码: {fund.fund_code}, 金额: {purchase_data.amount}, 手续费: {purchase_fee}"
        )

        return {
            "pending": PendingFundTransactionResponse.model_validate(pending),
            "transaction_info": {
                "申购金额": purchase_data.amount,
                "申购费用": purchase_fee,
                "实际投资金额": actual_amount,
                "交易状态": TRANSACTION_STATUS_PENDING,
                "有效交易日": str(effective_date),
                "可撤销截止": str(can_cancel_until),
            },
            "trading_info": trading_info,
        }


@router.post(
    "/holdings/redeem", response_model=FundTransactionResponse, tags=["基金持有"]
)
async def redeem_fund(
    redeem_data: FundRedeemRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """
    赎回基金

    交易规则：
    - 交易日15:00前提交：按当日净值确认
    - 交易日15:00后或非交易日提交：按下一个交易日净值确认
    - 赎回费用会从赎回金额中扣除
    - 一般基金赎回到账时间为T+1至T+7个工作日
    """
    # 获取持有记录
    holding = (
        db.query(UserFundHolding)
        .filter(
            UserFundHolding.id == redeem_data.holding_id,
            UserFundHolding.user_id == current_user.id,
            UserFundHolding.is_holding == True,
        )
        .first()
    )

    if not holding:
        raise HTTPException(status_code=404, detail="持有记录不存在")

    # 检查赎回份额是否超过持有份额
    if redeem_data.shares > holding.shares:
        raise HTTPException(status_code=400, detail="赎回份额不能超过持有份额")

    # 获取基金信息
    fund = db.query(FundBasic).filter(FundBasic.id == holding.fund_id).first()
    if not fund or not fund.latest_nav:
        raise HTTPException(status_code=400, detail="该基金暂无最新净值数据，无法赎回")

    # 检查最低赎回份额
    if fund.redemption_min_amount and redeem_data.shares < fund.redemption_min_amount:
        raise HTTPException(
            status_code=400,
            detail=f"赎回份额不能低于最低赎回份额 {fund.redemption_min_amount} 份",
        )

    # 获取交易日信息
    now = datetime.now()
    trading_info = get_trading_day_info(now)
    effective_date, transaction_status = get_effective_trading_day(now)

    # 计算赎回金额（基于当前最新净值作为参考）
    gross_redeem_amount = round(redeem_data.shares * fund.latest_nav, 2)

    # 计算赎回费用
    fee_rate = fund.redemption_fee or "0"
    # 如果交易可当日确认，则立即计算到账并更新持仓；否则写入等待表由批量任务确认
    if transaction_status == TRANSACTION_STATUS_CONFIRMED:
        actual_amount, redeem_fee = calculate_redeem_fee(gross_redeem_amount, fee_rate)
        actual_amount = round(actual_amount, 2)

        # 更新持有记录
        if abs(redeem_data.shares - holding.shares) < 0.0001:
            holding.is_holding = False
            holding.shares = 0
            holding.current_value = 0
        else:
            remaining_shares = holding.shares - redeem_data.shares
            remaining_cost = (holding.total_cost / holding.shares) * remaining_shares

            holding.shares = round(remaining_shares, 4)
            holding.total_cost = round(remaining_cost, 2)
            holding.current_price = fund.latest_nav

            calculate_holding_profit(holding, db)

        transaction = FundTransaction(
            user_id=current_user.id,
            fund_id=holding.fund_id,
            fund_code=holding.fund_code,
            fund_name=holding.fund_name,
            transaction_type=TransactionType.REDEEM,
            shares=redeem_data.shares,
            transaction_price=fund.latest_nav,
            transaction_amount=actual_amount,
            status=TRANSACTION_STATUS_CONFIRMED,
        )

        db.add(transaction)
        db.commit()
        db.refresh(transaction)

        logger.info(
            f"赎回基金已确认，用户ID: {current_user.id}, 基金代码: {holding.fund_code}, 份额: {redeem_data.shares}, 赎回费: {redeem_fee}"
        )

        return {
            "transaction": transaction,
            "redeem_info": {
                "赎回份额": redeem_data.shares,
                "确认净值": fund.latest_nav,
                "赎回总金额": round(gross_redeem_amount, 2),
                "赎回费用": round(redeem_fee, 2),
                "预计到账金额": actual_amount,
                "交易状态": TRANSACTION_STATUS_CONFIRMED,
                "有效交易日": str(effective_date),
                "预计到账时间": "T+1至T+7个工作日",
            },
            "trading_info": trading_info,
        }
    else:
        # 写入等待表，等待在 effective_date 当天 15:00 后系统批量确认
        can_cancel_until = datetime.combine(effective_date, TRADING_CUTOFF_TIME)
        pending = PendingFundTransaction(
            user_id=current_user.id,
            fund_id=fund.id,
            fund_code=holding.fund_code,
            fund_name=holding.fund_name,
            transaction_type=TransactionType.REDEEM,
            amount=redeem_data.shares,  # 存储赎回份额
            submit_time=now,
            effective_date=datetime.combine(effective_date, time(0, 0)),
            can_cancel_until=can_cancel_until,
            status=TRANSACTION_STATUS_PENDING,
            note=f"预计赎回份额: {redeem_data.shares}",
        )

        db.add(pending)
        db.commit()
        db.refresh(pending)

        transaction = FundTransaction(
            user_id=current_user.id,
            fund_id=holding.fund_id,
            fund_code=holding.fund_code,
            fund_name=holding.fund_name,
            transaction_type=TransactionType.REDEEM,
            shares=0,
            transaction_price=0,
            transaction_amount=gross_redeem_amount,
            status=TRANSACTION_STATUS_PENDING,
        )

        db.add(transaction)
        db.commit()

        logger.info(
            f"赎回已进入等待表，用户ID: {current_user.id}, 基金代码: {holding.fund_code}, 份额: {redeem_data.shares}"
        )

        return {
            "pending": PendingFundTransactionResponse.model_validate(pending),
            "redeem_info": {
                "赎回份额": redeem_data.shares,
                "确认净值": fund.latest_nav,
                "赎回总金额(参考)": round(gross_redeem_amount, 2),
                "交易状态": TRANSACTION_STATUS_PENDING,
                "有效交易日": str(effective_date),
                "可撤销截止": str(can_cancel_until),
            },
            "trading_info": trading_info,
        }


@router.get("/holdings", response_model=List[FundHoldingResponse], tags=["基金持有"])
async def get_fund_holdings(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
):
    """获取基金持有列表"""
    holdings = (
        db.query(UserFundHolding)
        .filter(
            UserFundHolding.user_id == current_user.id,
            UserFundHolding.is_holding == True,
        )
        .all()
    )

    # 更新最新净值和收益（包括日收益）
    for holding in holdings:
        fund = db.query(FundBasic).filter(FundBasic.id == holding.fund_id).first()
        if fund and fund.latest_nav:
            holding.current_price = fund.latest_nav
            calculate_holding_profit(holding, db)

    db.commit()

    return holdings


@router.get(
    "/transactions", response_model=List[FundTransactionResponse], tags=["交易记录"]
)
async def get_transaction_history(
    skip: int = Query(0, ge=0, description="跳过记录数"),
    limit: int = Query(100, ge=1, le=1000, description="返回记录数"),
    transaction_type: Optional[TransactionType] = Query(None, description="交易类型"),
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """获取交易记录"""
    query = db.query(FundTransaction).filter(FundTransaction.user_id == current_user.id)

    if transaction_type:
        query = query.filter(FundTransaction.transaction_type == transaction_type)

    transactions = (
        query.order_by(FundTransaction.transaction_time.desc())
        .offset(skip)
        .limit(limit)
        .all()
    )

    return transactions


@router.get("/total-profit", response_model=UserTotalProfitResponse, tags=["收益计算"])
async def get_total_profit(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
):
    """
    获取用户总收益

    收益计算说明：
    - 持有收益 = 当前市值 - 总成本
    - 持有收益率 = 持有收益 / 总成本 * 100%
    - 日收益 = 根据每只基金的当日涨幅计算（从FundRank或FundDaily表获取）
    """
    # 获取所有持有基金
    holdings = (
        db.query(UserFundHolding)
        .filter(
            UserFundHolding.user_id == current_user.id,
            UserFundHolding.is_holding == True,
        )
        .all()
    )

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
            # 计算收益（包括日收益，基于FundRank的daily_growth）
            calculate_holding_profit(holding, db)

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
    total_transaction_count = (
        db.query(FundTransaction)
        .filter(FundTransaction.user_id == current_user.id)
        .count()
    )

    return {
        "total_holding_value": round(total_holding_value, 2),
        "total_cost": round(total_cost, 2),
        "total_holding_profit": round(total_holding_profit, 2),
        "total_holding_profit_rate": round(total_holding_profit_rate, 2),
        "total_daily_profit": round(total_daily_profit, 2),
        "total_transaction_count": total_transaction_count,
        "total_holding_count": len(holdings),
    }


@router.get("/trading-day-info", tags=["交易信息"])
async def get_trading_day_status(
    current_user: User = Depends(get_current_user),
):
    """
    获取当前交易日信息

    返回信息包括：
    - 当前是否为交易日
    - 是否在15:00交易截止时间之前
    - 有效交易日是哪天
    - 交易状态（confirmed/pending）
    """
    return get_trading_day_info()


@router.get(
    "/pending-transactions",
    response_model=List[PendingFundTransactionResponse],
    tags=["等待交易"],
)
async def list_pending_transactions(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
):
    """列出当前用户的所有 pending 等待交易"""
    pendings = (
        db.query(PendingFundTransaction)
        .filter(PendingFundTransaction.user_id == current_user.id)
        .order_by(PendingFundTransaction.created_at.desc())
        .all()
    )
    return pendings


@router.post("/pending-transactions/{pending_id}/cancel", tags=["等待交易"])
async def cancel_pending_transaction(
    pending_id: int,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """取消 pending 交易（仅限 pending 状态且在 can_cancel_until 之前）"""
    p = (
        db.query(PendingFundTransaction)
        .filter(
            PendingFundTransaction.id == pending_id,
            PendingFundTransaction.user_id == current_user.id,
        )
        .first()
    )
    if not p:
        raise HTTPException(status_code=404, detail="等待交易不存在")
    if p.status != TRANSACTION_STATUS_PENDING:
        raise HTTPException(status_code=400, detail="交易不可取消（非 pending 状态）")
    if datetime.now() > p.can_cancel_until:
        raise HTTPException(status_code=400, detail="已超过可撤销截止时间，无法撤销")

    p.status = "canceled"
    db.commit()
    return {"status": "canceled", "pending_id": pending_id}


@router.post("/pending-transactions/process", tags=["等待交易"])
async def trigger_process_pending(
    current_user: User = Depends(get_current_user), db: Session = Depends(get_db)
):
    """触发处理 pending 交易（建议由定时任务在交易日15:00后调用）"""
    # 简单权限：仅允许管理员调用（如果 User role 可用）
    try:
        is_admin = hasattr(current_user, "role") or str(current_user.role) == "admin"
    except Exception:
        is_admin = False
    if not is_admin:
        raise HTTPException(status_code=403, detail="仅管理员或定时任务可触发批量处理")

    result = process_pending_transactions(db, date.today())
    return result


@router.get(
    "/funds/{fund_id_or_code}", response_model=FundInfoResponse, tags=["基金信息"]
)
async def get_fund_info(
    fund_id_or_code: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """获取基金详细信息"""
    # 尝试解析为整数（基金ID）
    try:
        fund_id = int(fund_id_or_code)
        fund = db.query(FundBasic).filter(FundBasic.id == fund_id).first()
    except ValueError:
        # 作为基金代码处理
        fund = (
            db.query(FundBasic).filter(FundBasic.fund_code == fund_id_or_code).first()
        )

    if not fund:
        raise HTTPException(status_code=404, detail="基金不存在")

    return fund
