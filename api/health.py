from fastapi import APIRouter
from loguru import logger

router = APIRouter()

@router.get("/")
async def check_health():
    """健康检查接口"""
    logger.info("健康检查请求")
    return {
        "status": "healthy",
        "service": "基金理财后端服务",
        "message": "服务运行正常"
    }
