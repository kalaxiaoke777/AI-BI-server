"""
FastAPI 调试启动脚本
直接运行此文件可以在调试模式下启动应用，无需使用 uvicorn --reload
"""

import uvicorn

if __name__ == "__main__":
    uvicorn.run(
        "app.main:app",
        host="127.0.0.1",
        port=8000,
        reload=False,  # 调试时关闭自动重载
        log_level="info",
    )
