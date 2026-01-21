from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config.config import settings
from loguru import logger

# 创建数据库引擎
engine = create_engine(
    settings.DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=3600,
)

# 创建会话工厂
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# 创建基类
Base = declarative_base()


# 获取数据库会话
def get_db():
    """获取数据库会话，用于 FastAPI 依赖注入"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# 初始化数据库
def init_db():
    """初始化数据库，创建所有表"""
    logger.info("初始化数据库")
    # 导入所有模型，确保它们被注册到 Base.metadata 中
    from db import models

    # 检查并更新表结构
    # 在开发环境中，我们可以使用drop_all和create_all来重新创建表
    # 在生产环境中，应该使用Alembic进行数据库迁移
    logger.info("开始更新数据库表结构...")

    # 先删除所有表，然后重新创建
    # Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)

    logger.info("数据库初始化完成")
