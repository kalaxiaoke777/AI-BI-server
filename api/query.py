from fastapi import APIRouter
from loguru import logger

router = APIRouter()


@router.get("/")
async def query():
    """健康检查接口"""
    return {"status": "query", "service": "查询接口", "message": "服务运行正常"}
