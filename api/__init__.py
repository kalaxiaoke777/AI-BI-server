from fastapi import APIRouter
from api import fund, health, scrape, query, userManager, SsFundManager

# 创建主路由
router = APIRouter()

# 注册子路由
router.include_router(health.router, prefix="/health", tags=["健康检查"])
router.include_router(fund.router, prefix="/fund", tags=["基金数据"])
router.include_router(scrape.router, prefix="/scrape", tags=["数据采集"])
router.include_router(query.router, prefix="/query", tags=["查询接口"])
router.include_router(userManager.router, prefix="/user", tags=["用户管理"])
router.include_router(SsFundManager.router, prefix="/ss-fund", tags=["自选基金"])
