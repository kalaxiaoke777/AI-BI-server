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

    # OpenAI / 兼容接口配置
    OPENAI_API_KEY: Optional[str] = (
        "sk-ct5hha04elcxd9einsuxjnebcppy3g4opqi5vn3h7ddmrgc2"
    )
    OPENAI_BASE_URL: Optional[str] = "https://api.xiaomimimo.com/v1"
    OPENAI_MODEL: str = "mimo-v2-flash"

    GETFUND_API_URL: str = (
        "https://m.dayfund.cn/ajs/ajaxdata.shtml?showtype=getfundvalue&fundcode="
    )
    headers: dict = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://fund.eastmoney.com/",
    }  # 加headers防反爬

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True


# 创建配置实例
settings = Settings()
