from sqlalchemy import Column, Integer, String, Text, DateTime, Float, ForeignKey, Boolean, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from db import Base
import enum

# Data type enum
class DataType(str, enum.Enum):
    FUND_BASIC = "fund_basic"  # Fund basic info
    FUND_DAILY = "fund_daily"  # Fund daily data
    FUND_HOLDINGS = "fund_holdings"  # Fund holdings
    FUND_RATING = "fund_rating"  # Fund rating
    OTHER = "other"  # Other type

# Data source enum
class DataSource(str, enum.Enum):
    EASTMONEY = "eastmoney"  # East Money
    TIANTIAN = "tiantian"  # Tian Tian Fund
    XUEQIU = "xueqiu"  # Xue Qiu
    ANT = "ant"  # Ant Fortune
    OTHER = "other"  # Other source

# Fund basic info table
class FundBasic(Base):
    __tablename__ = "fund_basic"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    fund_code = Column(String(20), unique=True, index=True, nullable=False, comment="fund_code")
    short_name = Column(String(50), comment="short_name")
    fund_name = Column(String(100), index=True, nullable=False, comment="fund_name")
    fund_type = Column(Integer, comment="fund_type")
    pinyin = Column(String(200), comment="pinyin")
    manager = Column(String(100), comment="manager")
    company_id = Column(Integer, ForeignKey("fund_company.id"), nullable=True, index=True, comment="company_id")
    company_name = Column(String(100), comment="company_name")  # For backward compatibility
    launch_date = Column(DateTime, comment="成立日期")
    latest_nav = Column(Float, comment="latest_nav")
    latest_nav_date = Column(DateTime, comment="latest_nav_date")
    is_purchaseable = Column(Boolean, default=True, comment="is_purchaseable")
    purchase_start_date = Column(DateTime, comment="purchase_start_date")
    purchase_end_date = Column(DateTime, comment="purchase_end_date")
    purchase_min_amount = Column(Float, comment="purchase_min_amount")
    redemption_min_amount = Column(Float, comment="redemption_min_amount")
    risk_level = Column(Float, comment="risk_level")
    purchase_fee = Column(String(10), default="0", comment="申购费率%")
    redemption_fee = Column(String(10), default="0", comment="赎回费率%")
    purchase_fee_rate = Column(String(10), default="0", comment="优惠后申购费率%")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), comment="updated_at")
    
    # Relationships
    daily_data = relationship("FundDaily", back_populates="fund")
    holdings = relationship("FundHolding", back_populates="fund")
    raw_data = relationship("RawFundData", back_populates="fund_basic")
    company = relationship("FundCompany", back_populates="funds")
    growths = relationship("FundGrowth", back_populates="fund")
    ranks = relationship("FundRank", back_populates="fund")

# Fund daily data table
class FundDaily(Base):
    __tablename__ = "fund_daily"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("fund_basic.id"), nullable=False, index=True, comment="fund_id")
    trade_date = Column(DateTime, nullable=False, index=True, comment="trade_date")
    nav = Column(Float, comment="nav")
    accum_nav = Column(Float, comment="accum_nav")
    daily_growth = Column(Float, comment="daily_growth")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    
    # Relationships
    fund = relationship("FundBasic", back_populates="daily_data")

# Fund company table
class FundCompany(Base):
    __tablename__ = "fund_company"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    company_code = Column(String(20), unique=True, index=True, nullable=False, comment="company_code")
    company_name = Column(String(100), index=True, nullable=False, comment="company_name")
    short_name = Column(String(50), comment="short_name")
    establish_date = Column(DateTime, comment="establish_date")
    registered_capital = Column(Float, comment="registered_capital")
    address = Column(String(200), comment="address")
    contact_phone = Column(String(50), comment="contact_phone")
    website = Column(String(200), comment="website")
    description = Column(Text, comment="description")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), comment="updated_at")
    
    # Relationships
    funds = relationship("FundBasic", back_populates="company")

# Fund holdings table
class FundHolding(Base):
    __tablename__ = "fund_holding"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("fund_basic.id"), nullable=False, index=True, comment="fund_id")
    stock_code = Column(String(20), comment="stock_code")
    stock_name = Column(String(100), comment="stock_name")
    holding_ratio = Column(Float, comment="holding_ratio")
    report_date = Column(DateTime, comment="report_date")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    
    # Relationships
    fund = relationship("FundBasic", back_populates="holdings")

# Fund growth table
class FundGrowth(Base):
    __tablename__ = "fund_growth"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("fund_basic.id"), nullable=False, index=True, comment="fund_id")
    daily_growth = Column(Float, comment="近1日涨幅")
    weekly_growth = Column(Float, comment="近1周涨幅")
    monthly_growth = Column(Float, comment="近1月涨幅")
    quarterly_growth = Column(Float, comment="近3月涨幅")
    yearly_growth = Column(Float, comment="近1年涨幅")
    update_date = Column(DateTime, nullable=False, index=True, comment="update_date")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), comment="updated_at")
    
    # Relationships
    fund = relationship("FundBasic", back_populates="growths")

# Fund rank table
class FundRank(Base):
    __tablename__ = "fund_rank"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    fund_id = Column(Integer, ForeignKey("fund_basic.id"), nullable=False, index=True, comment="fund_id")
    rank_date = Column(DateTime, nullable=False, index=True, comment="rank_date")
    rank = Column(Integer, comment="rank")
    rank_type = Column(String(50), index=True, comment="rank_type")
    nav = Column(Float, comment="单位净值")
    accum_nav = Column(Float, comment="累计净值")
    daily_growth = Column(Float, comment="近1日涨幅")
    weekly_growth = Column(Float, comment="近1周涨幅")
    monthly_growth = Column(Float, comment="近1月涨幅")
    quarterly_growth = Column(Float, comment="近3月涨幅")
    yearly_growth = Column(Float, comment="近1年涨幅")
    two_year_growth = Column(Float, comment="近2年涨幅")
    three_year_growth = Column(Float, comment="近3年涨幅")
    five_year_growth = Column(Float, comment="近5年涨幅")
    ytd_growth = Column(Float, comment="今年以来涨幅")
    since_launch_growth = Column(Float, comment="成立以来增长率")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), comment="updated_at")
    
    # Relationships
    fund = relationship("FundBasic", back_populates="ranks")

# Raw fund data table (for storing crawled raw data)
class RawFundData(Base):
    __tablename__ = "raw_fund_data"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    fund_code = Column(String(20), index=True, comment="fund_code")
    fund_id = Column(Integer, ForeignKey("fund_basic.id"), nullable=True, index=True, comment="fund_id")
    data_type = Column(Enum(DataType), nullable=False, index=True, comment="data_type")
    source = Column(Enum(DataSource), nullable=False, index=True, comment="source")
    source_url = Column(String(500), comment="source_url")
    raw_content = Column(Text, nullable=False, comment="raw_content")
    is_processed = Column(Boolean, default=False, index=True, comment="is_processed")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), index=True, comment="created_at")
    
    # Relationships
    fund_basic = relationship("FundBasic", back_populates="raw_data")

# Scrape task table
class ScrapeTask(Base):
    __tablename__ = "scrape_task"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    task_id = Column(String(100), unique=True, index=True, comment="task_id")
    source = Column(Enum(DataSource), nullable=False, index=True, comment="source")
    data_type = Column(Enum(DataType), nullable=False, index=True, comment="data_type")
    status = Column(String(20), default="pending", index=True, comment="status")
    start_time = Column(DateTime(timezone=True), nullable=True, comment="start_time")
    end_time = Column(DateTime(timezone=True), nullable=True, comment="end_time")
    total_count = Column(Integer, default=0, comment="total_count")
    success_count = Column(Integer, default=0, comment="success_count")
    error_count = Column(Integer, default=0, comment="error_count")
    error_message = Column(Text, comment="error_message")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    
    # Relationships
    task_items = relationship("ScrapeTaskItem", back_populates="task")

# Scrape task item table
class ScrapeTaskItem(Base):
    __tablename__ = "scrape_task_item"
    
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    task_id = Column(Integer, ForeignKey("scrape_task.id"), nullable=False, index=True, comment="task_id")
    fund_code = Column(String(20), index=True, comment="fund_code")
    status = Column(String(20), default="pending", index=True, comment="status")
    error_message = Column(Text, comment="error_message")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="created_at")
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), server_default=func.now(), comment="updated_at")
    
    # Relationships
    task = relationship("ScrapeTask", back_populates="task_items")
