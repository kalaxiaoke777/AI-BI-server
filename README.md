# Fund Financial Backend Service

基金理财后端服务，用于基金数据的采集、存储和管理。

## 技术栈

- Python 3.10+
- FastAPI
- SQLAlchemy 2.0
- PostgreSQL
- Pydantic
- Loguru
- Requests
- BeautifulSoup4
- Scrapy

## 项目结构

```
.
├── api/              # API 路由层
│   ├── __init__.py   # API 路由初始化
│   ├── fund.py       # 基金相关 API
│   ├── health.py     # 健康检查 API
│   └── scrape.py     # 数据采集相关 API
├── app/              # 应用核心层
│   ├── scrapers/     # 爬虫模块
│   │   ├── base.py   # 抽象爬虫基类
│   │   └── eastmoney.py # 东方财富爬虫实现
│   ├── services/     # 业务服务层
│   │   └── scrape_service.py # 数据采集服务
│   └── main.py       # FastAPI 应用入口
├── config/           # 配置管理
│   └── config.py     # 配置类定义
├── db/               # 数据库相关
│   ├── __init__.py   # 数据库连接初始化
│   └── models.py     # 数据库模型定义
├── .env              # 环境变量配置
├── requirements.txt  # 项目依赖
└── README.md         # 项目说明文档
```

## 主要功能

1. **基金数据采集**
   - 支持从东方财富等多个数据源采集基金数据
   - 支持采集基金基础信息、日线数据、持仓数据等
   - 支持采集单只或多只基金数据
   - 支持采集所有基金数据

2. **数据存储**
   - 原始数据存储
   - 结构化数据存储
   - 采集任务跟踪

3. **API 接口**
   - 健康检查
   - 基金数据查询
   - 数据采集触发
   - 采集任务状态查询
   - 采集历史记录查询

## 环境配置

1. 复制 `.env` 文件并根据实际情况修改配置：
   ```
   cp .env.example .env
   ```

2. 安装依赖：
   ```
   pip install -r requirements.txt
   ```

3. 确保 PostgreSQL 数据库已创建，且配置正确。

## 运行项目

1. 启动 FastAPI 应用：
   ```
   uvicorn app.main:app --reload
   ```

2. 访问 API 文档：
   ```
   http://localhost:8000/docs
   ```

## API 示例

### 1. 健康检查
```
GET /health
```

### 2. 触发采集所有基金数据
```
POST /api/v1/scrape/funds/all?source=eastmoney&data_type=fund_basic
```

### 3. 触发采集指定基金数据
```
POST /api/v1/scrape/funds
Content-Type: application/json

{
  "source": "eastmoney",
  "data_type": "fund_basic",
  "fund_code_list": ["000001", "000002"]
}
```

## 开发说明

1. **添加新爬虫**：
   - 继承 `BaseScraper` 抽象类
   - 实现 `fetch_data`、`parse_data` 和 `get_data_url` 方法
   - 在 `ScrapeService` 中注册新爬虫

2. **添加新数据类型**：
   - 在 `DataType` 枚举中添加新类型
   - 更新数据库模型
   - 实现相应的解析逻辑

3. **添加新 API**：
   - 在 `api/` 目录下创建新的路由文件
   - 在 `api/__init__.py` 中注册路由

## 注意事项

1. 首次运行时，数据库表会自动创建。
2. 运行爬虫前，请确保网络连接正常，且数据源网站可访问。
3. 采集大量基金数据时，建议分批进行，避免对数据源造成过大压力。
4. 定期清理日志文件，避免占用过多磁盘空间。
