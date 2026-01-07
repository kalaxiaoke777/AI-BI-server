"""AI（OpenAI SDK）相关接口。

说明：
- 该模块提供最简 GET 接口，基于 OpenAI Python SDK。
- API Key/BASE_URL 不写死在代码中，通过环境变量或 .env 配置。
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from loguru import logger
from openai import OpenAI

from config.config import settings


router = APIRouter(
    tags=["ai"],
    responses={404: {"description": "Not found"}},
)


@router.get("/ping")
async def ai_ping():
    """最简健康检查接口。"""
    return {"status": "ok", "service": "ai"}


@router.get("/ask")
async def ai_ask(
    prompt: str = Query(..., min_length=1, description="用户输入"),
    model: str | None = Query(None, description="模型名称，缺省使用配置"),
    temperature: float = Query(0.3, ge=0.0, le=2.0, description="采样温度"),
    max_tokens: int = Query(512, ge=1, le=4096, description="最大输出 token 数"),
):
    """简易 GET 接口：调用大模型返回一段文本。"""

    api_key = getattr(settings, "OPENAI_API_KEY", None)
    base_url = getattr(settings, "OPENAI_BASE_URL", None)
    default_model = getattr(settings, "OPENAI_MODEL", None)

    if not api_key:
        raise HTTPException(
            status_code=503,
            detail="OPENAI_API_KEY 未配置（请在 .env 或环境变量中设置）",
        )

    try:
        client = OpenAI(api_key=api_key, base_url=base_url or None)
        completion = client.chat.completions.create(
            model=model or default_model or "mimo-v2-flash",
            messages=[
                {"role": "system", "content": "你是一个有帮助的智能助手。"},
                {"role": "user", "content": prompt},
            ],
            max_completion_tokens=max_tokens,
            temperature=temperature,
            top_p=0.95,
            stream=False,
        )

        content = (completion.choices[0].message.content or "").strip()
        return {"prompt": prompt, "answer": content}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"AI 请求失败: {str(e)}")
        raise HTTPException(status_code=500, detail="AI 请求失败")
