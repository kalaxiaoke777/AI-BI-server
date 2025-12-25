from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger
from config.config import settings
from api import router as api_router

# 配置日志
logger.add(
    settings.LOG_FILE,
    rotation="1 week",
    retention="4 weeks",
    level=settings.LOG_LEVEL,
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)

# 创建 FastAPI 应用
app = FastAPI(
    title="Fund Financial Backend Service",
    description="Fund Data Collection and Management API",
    version="1.0.0"
)

# 配置 CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 注册路由
app.include_router(api_router, prefix="/api/v1")

# 健康检查路由
@app.get("/health")
async def health_check():
    return {"status": "healthy", "message": "基金理财后端服务运行正常"}

# 启动事件
@app.on_event("startup")
async def startup_event():
    logger.info("启动基金理财后端服务")
    logger.info(f"日志级别: {settings.LOG_LEVEL}")
    logger.info(f"数据库 URL: {settings.DATABASE_URL}")
    
    # 初始化数据库
    from db import init_db
    init_db()

# 关闭事件
@app.on_event("shutdown")
async def shutdown_event():
    logger.info("关闭基金理财后端服务")
