from fastapi import APIRouter
from api import fund, health, scrape

# 创建主路由
router = APIRouter()

# 注册子路由
router.include_router(health.router, prefix="/health", tags=["健康检查"])
router.include_router(fund.router, prefix="/fund", tags=["基金数据"])
router.include_router(scrape.router, prefix="/scrape", tags=["数据采集"])
