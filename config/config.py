from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # 应用配置
    APP_NAME: str = "Fund Financial Backend Service"
    APP_VERSION: str = "1.0.0"
    DEBUG: bool = True
    
    # 数据库配置
    DATABASE_URL: str = "postgresql://postgres:123456@localhost:5432/fund_ai"
    
    # 日志配置
    LOG_FILE: str = "logs/app.log"
    LOG_LEVEL: str = "INFO"
    
    # 爬虫配置
    SCRAPY_SETTINGS_MODULE: str = "scrapy_spiders.settings"
    
    # API 配置
    API_V1_STR: str = "/api/v1"
    
    # 调度配置
    SCHEDULE_INTERVAL: int = 3600  # 秒
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

# 创建配置实例
settings = Settings()
