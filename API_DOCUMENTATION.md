# 基金理财后端服务 API 文档

## 项目概述

基金理财后端服务是一个基于 FastAPI 开发的基金数据管理系统，提供基金数据采集、查询、用户管理和基金交易模拟等功能。

## 基础信息

### 基本 URL

- **开发环境**: `http://localhost:8000/api/v1`
- **生产环境**: 待配置

### 认证方式

- **JWT Token**: 用户登录后获取访问令牌，在请求中通过 `token` 查询参数传递
- **认证流程**: 
  1. 用户通过 `/api/v1/user/login` 接口登录，获取 `access_token`
  2. 后续请求在 URL 中添加 `?token={access_token}`

## 接口分类

### 2. 基金数据

#### 基金列表与详情

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 获取基金列表 | GET | `/api/v1/fund/` | 获取基金列表，支持分页和过滤 |
| 获取基金详情 | GET | `/api/v1/fund/{fund_id}` | 获取单个基金的详细信息 |
| 获取基金历史涨幅 | GET | `/api/v1/fund/{fund_id}/growth` | 获取基金历史涨幅数据 |

#### 基金公司相关

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 获取基金公司列表 | GET | `/api/v1/fund/companies` | 获取基金公司列表 |
| 获取基金公司详情 | GET | `/api/v1/fund/companies/{company_id}` | 获取基金公司详细信息 |
| 获取公司基金列表 | GET | `/api/v1/fund/companies/{company_id}/funds` | 获取基金公司发行的基金列表 |

#### 基金数据导入与更新

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 导入基金列表 | POST | `/api/v1/fund/import` | 导入基金列表（仅初始化使用） |
| 导入基金公司 | POST | `/api/v1/fund/company/import` | 导入基金公司列表 |
| 导入基金排行 | POST | `/api/v1/fund/rank/import` | 导入基金排行数据 |
| 更新基金涨幅 | POST | `/api/v1/fund/growth/update` | 更新基金历史涨幅数据 |
| 更新基金排行 | POST | `/api/v1/fund/rank/update` | 更新基金排行数据 |
| 同步基金公司关系 | POST | `/api/v1/fund/sync-company-relation` | 同步基金和基金公司的关联关系 |

### 3. 数据采集

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 触发基金数据采集 | POST | `/api/v1/scrape/funds` | 触发基金数据采集任务 |
| 触发所有基金采集 | POST | `/api/v1/scrape/funds/all` | 触发采集所有基金的数据 |
| 获取采集任务状态 | GET | `/api/v1/scrape/status/{task_id}` | 获取采集任务状态 |
| 获取采集历史记录 | GET | `/api/v1/scrape/history` | 获取采集历史记录 |

### 4. 查询接口

#### 基金公司查询

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 查询基金公司列表 | GET | `/api/v1/query/fund/company` | 查询基金公司列表，支持分页、模糊查询和排序 |
| 查询基金公司详情 | GET | `/api/v1/query/fund/company/{company_id}` | 查询基金公司详情 |

#### 基金基本信息查询

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 查询基金基本信息 | GET | `/api/v1/query/fund/basic` | 查询基金基本信息列表，支持分页、多条件过滤和排序 |
| 查询基金基本详情 | GET | `/api/v1/query/fund/basic/{fund_id}` | 查询基金基本信息详情，包含公司信息 |

#### 基金排行查询

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 查询基金排行列表 | GET | `/api/v1/query/fund/rank` | 查询基金排行列表，支持分页、多条件过滤和排序 |

#### 基金增长率查询

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 查询基金增长率 | GET | `/api/v1/query/fund/growth` | 查询基金增长率数据，支持分页、多条件过滤和排序 |

#### 组合查询

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 组合查询基金数据 | GET | `/api/v1/query/fund/combined` | 组合查询基金数据，包含基本信息、排行数据和公司信息 |

### 5. 用户管理

#### 用户认证

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 用户登录 | POST | `/api/v1/user/login` | 用户登录，获取访问令牌 |
| 用户注册 | POST | `/api/v1/user/register` | 注册新用户 |

#### 用户信息

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 获取当前用户信息 | GET | `/api/v1/user/me` | 获取当前登录用户信息 |
| 获取所有用户列表 | GET | `/api/v1/user/users` | 获取所有用户列表（仅管理员可访问） |
| 获取指定用户信息 | GET | `/api/v1/user/users/{user_id}` | 获取指定用户信息（仅管理员可访问） |
| 更新用户信息 | PUT | `/api/v1/user/users/{user_id}` | 更新用户信息（仅管理员可访问） |
| 删除用户 | DELETE | `/api/v1/user/users/{user_id}` | 删除用户（仅管理员可访问） |

### 6. 自选基金与交易

#### 自选基金

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 添加自选基金 | POST | `/api/v1/ss-fund/favorite-funds` | 添加基金到自选列表 |
| 获取自选基金列表 | GET | `/api/v1/ss-fund/favorite-funds` | 获取自选基金列表 |
| 移除自选基金 | DELETE | `/api/v1/ss-fund/favorite-funds/{favorite_id}` | 从自选列表中移除基金 |

#### 基金持有

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 购买基金 | POST | `/api/v1/ss-fund/holdings/purchase` | 购买基金 |
| 赎回基金 | POST | `/api/v1/ss-fund/holdings/redeem` | 赎回基金 |
| 获取持有基金列表 | GET | `/api/v1/ss-fund/holdings` | 获取当前持有的基金列表 |

#### 交易记录

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 获取交易记录 | GET | `/api/v1/ss-fund/transactions` | 获取交易历史记录 |
| 获取总收益 | GET | `/api/v1/ss-fund/total-profit` | 获取用户总收益 |

#### 基金信息

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 获取基金详细信息 | GET | `/api/v1/ss-fund/funds/{fund_id_or_code}` | 获取基金详细信息 |

### 7. 指数数据

| 接口名称 | 请求方法 | URL | 功能描述 |
|---------|---------|-----|---------|
| 获取指数列表 | GET | `/api/v1/index/list` | 获取指数列表 |
| 获取指数历史数据 | GET | `/api/v1/index/history` | 获取指定指数的历史数据 |
| 获取指数详情 | GET | `/api/v1/index/info/{index_name}` | 获取指数详细信息 |
| 同步指数历史数据 | POST | `/api/v1/index/sync` | 同步指数历史数据 |

## 接口详细说明

### 1. 健康检查接口

#### GET /api/v1/health

**响应示例**:
```json
{
  "status": "healthy",
  "service": "基金理财后端服务",
  "message": "服务运行正常"
}
```

### 2. 用户认证接口

#### POST /api/v1/user/login

**请求体**:
```json
{
  "username": "admin",
  "password": "password123"
}
```

**响应示例**:
```json
{
  "access_token": "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9...",
  "token_type": "bearer",
  "user_info": {
    "id": 1,
    "username": "admin",
    "email": "admin@example.com",
    "role": "admin",
    "is_active": true,
    "created_at": "2025-12-01T00:00:00",
    "updated_at": "2025-12-01T00:00:00"
  }
}
```

### 2. 健康检查接口

#### GET /api/v1/health

**功能描述**: 检查服务健康状态

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/health
```

**响应示例**:
```json
{
  "status": "healthy",
  "service": "基金理财后端服务",
  "message": "服务运行正常"
}
```

### 3. 基金数据接口

#### GET /api/v1/fund/

**功能描述**: 获取基金列表，支持分页和过滤

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页大小 |
| fund_code | string | 否 | None | 基金代码 |
| fund_name | string | 否 | None | 基金名称（支持模糊查询） |
| fund_type | string | 否 | None | 基金类型 |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/fund/?page=1&page_size=10&fund_name=易方达"
```

**响应示例**:
```json
{
  "total": 100,
  "page": 1,
  "page_size": 10,
  "data": [
    {
      "id": 1,
      "fund_code": "000001",
      "short_name": "华夏成长混合",
      "fund_name": "华夏成长混合A",
      "fund_type": "1",
      "pinyin": "huaxiagrowth",
      "manager": "张某某",
      "company": "华夏基金",
      "establish_date": "2001-01-01T00:00:00",
      "latest_nav": 1.2345,
      "latest_nav_date": "2025-12-01T00:00:00",
      "created_at": "2025-12-01T00:00:00",
      "updated_at": "2025-12-01T00:00:00"
    }
  ]
}
```

#### GET /api/v1/fund/{fund_id}

**功能描述**: 获取单个基金的详细信息

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| fund_id | int | 是 | 基金ID |

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/fund/1
```

**响应示例**:
```json
{
  "id": 1,
  "fund_code": "000001",
  "short_name": "华夏成长混合",
  "fund_name": "华夏成长混合A",
  "fund_type": "1",
  "pinyin": "huaxiagrowth",
  "manager": "张某某",
  "company": "华夏基金",
  "establish_date": "2001-01-01T00:00:00",
  "latest_nav": 1.2345,
  "latest_nav_date": "2025-12-01T00:00:00",
  "created_at": "2025-12-01T00:00:00",
  "updated_at": "2025-12-01T00:00:00"
}
```

#### POST /api/v1/fund/import

**功能描述**: 导入基金列表（仅初始化使用，不覆盖已有数据）

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| source | string | 是 | 数据源，如eastmoney |

**请求示例**:
```bash
curl -X POST "http://localhost:8000/api/v1/fund/import?source=eastmoney"
```

**响应示例**:
```json
{
  "status": "success",
  "message": "基金列表导入完成",
  "data": {
    "total_count": 1000,
    "success_count": 1000,
    "failed_count": 0
  }
}
```

#### POST /api/v1/fund/company/import

**功能描述**: 导入基金公司列表（仅初始化使用，不覆盖已有数据）

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| source | string | 是 | 数据源，如eastmoney |

**请求示例**:
```bash
curl -X POST "http://localhost:8000/api/v1/fund/company/import?source=eastmoney"
```

**响应示例**:
```json
{
  "status": "success",
  "message": "基金公司列表导入完成",
  "data": {
    "total_count": 100,
    "success_count": 100,
    "failed_count": 0
  }
}
```

#### POST /api/v1/fund/rank/import

**功能描述**: 导入基金排行数据（仅初始化使用）

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| source | string | 是 | 数据源，如eastmoney |
| max_pages | int | 否 | 最大页码，为None时获取所有数据 |

**请求示例**:
```bash
curl -X POST "http://localhost:8000/api/v1/fund/rank/import?source=eastmoney&max_pages=10"
```

**响应示例**:
```json
{
  "status": "success",
  "message": "基金排行数据导入完成",
  "data": {
    "total_count": 1000,
    "success_count": 1000,
    "failed_count": 0
  }
}
```

### 4. 数据采集接口

#### POST /api/v1/scrape/funds

**功能描述**: 触发基金数据采集

**请求体**:
```json
{
  "source": "eastmoney",
  "data_type": "fund_basic",
  "fund_code_list": ["000001", "000002"]
}
```

**请求示例**:
```bash
curl -X POST http://localhost:8000/api/v1/scrape/funds \
  -H "Content-Type: application/json" \
  -d '{"source": "eastmoney", "data_type": "fund_basic", "fund_code_list": ["000001", "000002"]}'
```

**响应示例**:
```json
{
  "status": "success",
  "message": "基金数据采集完成",
  "task_id": "task_1234567890",
  "success_count": 2,
  "error_count": 0
}
```

#### POST /api/v1/scrape/funds/all

**功能描述**: 触发采集所有基金的数据

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| source | string | 是 | 数据源，如eastmoney |
| data_type | string | 是 | 数据类型，如fund_basic |

**请求示例**:
```bash
curl -X POST "http://localhost:8000/api/v1/scrape/funds/all?source=eastmoney&data_type=fund_basic"
```

**响应示例**:
```json
{
  "status": "success",
  "message": "所有基金数据采集完成",
  "task_id": "task_0987654321",
  "success_count": 1000,
  "error_count": 0
}
```

#### GET /api/v1/scrape/status/{task_id}

**功能描述**: 获取采集任务状态

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| task_id | string | 是 | 任务ID |

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/scrape/status/task_1234567890
```

**响应示例**:
```json
{
  "status": "success",
  "task": {
    "task_id": "task_1234567890",
    "source": "eastmoney",
    "data_type": "fund_basic",
    "status": "completed",
    "start_time": "2025-12-01T00:00:00",
    "end_time": "2025-12-01T00:01:00",
    "total_count": 2,
    "success_count": 2,
    "error_count": 0
  }
}
```

#### GET /api/v1/scrape/history

**功能描述**: 获取采集历史记录

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页大小 |
| source | string | 否 | None | 数据源 |
| data_type | string | 否 | None | 数据类型 |
| status | string | 否 | None | 任务状态 |
| start_date | string | 否 | None | 开始日期 |
| end_date | string | 否 | None | 结束日期 |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/scrape/history?page=1&page_size=10&source=eastmoney"
```

**响应示例**:
```json
{
  "status": "success",
  "data": {
    "total": 50,
    "page": 1,
    "page_size": 10,
    "data": [
      {
        "task_id": "task_1234567890",
        "source": "eastmoney",
        "data_type": "fund_basic",
        "status": "completed",
        "start_time": "2025-12-01T00:00:00",
        "end_time": "2025-12-01T00:01:00",
        "total_count": 2,
        "success_count": 2,
        "error_count": 0
      }
    ]
  }
}
```

### 5. 查询接口

#### GET /api/v1/query/fund/company

**功能描述**: 查询基金公司列表，支持分页、模糊查询和排序

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页条数 |
| company_name | string | 否 | None | 基金公司名称，支持模糊查询 |
| company_code | string | 否 | None | 基金公司代码 |
| sort_by | string | 否 | None | 排序字段，如 establish_date, created_at |
| sort_order | string | 否 | asc | 排序方式，asc 或 desc |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/query/fund/company?page=1&page_size=10&company_name=华夏"
```

**响应示例**:
```json
{
  "total": 10,
  "page": 1,
  "page_size": 10,
  "total_pages": 1,
  "data": [
    {
      "id": 1,
      "company_code": "000001",
      "company_name": "华夏基金管理有限公司",
      "short_name": "华夏基金",
      "establish_date": "1998-04-09",
      "registered_capital": "138000000.0",
      "address": "北京市西城区金融大街33号通泰大厦B座10层",
      "contact_phone": "010-88066688",
      "website": "www.chinaamc.com",
      "description": "华夏基金管理有限公司成立于1998年4月9日，是经中国证监会批准成立的首批全国性基金管理公司之一。",
      "created_at": "2025-12-01T00:00:00",
      "updated_at": "2025-12-01T00:00:00"
    }
  ]
}
```

#### GET /api/v1/query/fund/company/{company_id}

**功能描述**: 查询基金公司详情

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| company_id | int | 是 | 基金公司ID |

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/query/fund/company/1
```

**响应示例**:
```json
{
  "id": 1,
  "company_code": "000001",
  "company_name": "华夏基金管理有限公司",
  "short_name": "华夏基金",
  "establish_date": "1998-04-09",
  "registered_capital": "138000000.0",
  "address": "北京市西城区金融大街33号通泰大厦B座10层",
  "contact_phone": "010-88066688",
  "website": "www.chinaamc.com",
  "description": "华夏基金管理有限公司成立于1998年4月9日，是经中国证监会批准成立的首批全国性基金管理公司之一。",
  "created_at": "2025-12-01T00:00:00",
  "updated_at": "2025-12-01T00:00:00",
  "fund_count": 50
}
```

#### GET /api/v1/query/fund/basic

**功能描述**: 查询基金基本信息，支持分页、过滤和排序

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页条数 |
| fund_code | string | 否 | None | 基金代码 |
| fund_name | string | 否 | None | 基金名称，支持模糊查询 |
| fund_type | string | 否 | None | 基金类型 |
| risk_level | string | 否 | None | 风险等级 |
| sort_by | string | 否 | None | 排序字段，如 latest_nav, year_growth_rate |
| sort_order | string | 否 | asc | 排序方式，asc 或 desc |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/query/fund/basic?page=1&page_size=10&fund_name=易方达&sort_by=year_growth_rate&sort_order=desc"
```

**响应示例**:
```json
{
  "total": 100,
  "page": 1,
  "page_size": 10,
  "total_pages": 10,
  "data": [
    {
      "id": 1,
      "fund_code": "000001",
      "fund_name": "易方达蓝筹精选混合",
      "short_name": "易方达蓝筹精选",
      "fund_type": "混合型",
      "risk_level": "中高风险",
      "establish_date": "2018-09-05",
      "latest_nav": 2.5678,
      "latest_nav_date": "2025-12-01",
      "year_growth_rate": "35.21",
      "purchase_fee": "1.5",
      "redemption_fee": "0.5",
      "manager": "张坤",
      "company": "易方达基金管理有限公司",
      "created_at": "2025-12-01T00:00:00",
      "updated_at": "2025-12-01T00:00:00"
    }
  ]
}
```

### 6. 用户管理接口

#### POST /api/v1/user/login

**功能描述**: 用户登录

**请求体**:
```json
{
  "username": "admin",
  "password": "password123"
}
```

**请求示例**:
```bash
curl -X POST http://localhost:8000/api/v1/user/login \
  -H "Content-Type: application/json" \
  -d '{"username": "admin", "password": "password123"}'
```

**响应示例**:
```json
{
  "status": "success",
  "message": "登录成功",
  "data": {
    "access_token": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9...",
    "token_type": "bearer",
    "expires_in": 3600,
    "user": {
      "id": 1,
      "username": "admin",
      "email": "admin@example.com",
      "role": "admin",
      "created_at": "2025-12-01T00:00:00"
    }
  }
}
```

#### POST /api/v1/user/register

**功能描述**: 用户注册

**请求体**:
```json
{
  "username": "newuser",
  "email": "newuser@example.com",
  "password": "password123",
  "confirm_password": "password123"
}
```

**请求示例**:
```bash
curl -X POST http://localhost:8000/api/v1/user/register \
  -H "Content-Type: application/json" \
  -d '{"username": "newuser", "email": "newuser@example.com", "password": "password123", "confirm_password": "password123"}'
```

**响应示例**:
```json
{
    "id": 4,
    "username": "kalaxiaoke33",
    "email": "user@example.com",
    "role": "admin",
    "is_active": true,
    "created_at": "2025-12-29T22:11:54.837166+08:00",
    "updated_at": "2025-12-29T22:11:54.837166+08:00"
}
```

### 7. 自选基金与交易接口

#### GET /api/v1/user/favorite-funds

**功能描述**: 获取用户的自选基金列表

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页大小 |

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/user/favorite-funds \
  -H "Authorization: Bearer your_access_token"
```

**响应示例**:
```json
{
  "total": 5,
  "page": 1,
  "page_size": 10,
  "data": [
    {
      "id": 1,
      "user_id": 1,
      "fund_id": 1,
      "fund_code": "000001",
      "fund_name": "易方达蓝筹精选混合",
      "short_name": "易方达蓝筹精选",
      "created_at": "2025-12-01T00:00:00"
    }
  ]
}
```

#### POST /api/v1/user/favorite-funds

**功能描述**: 添加基金到自选

**请求体**:
```json
{
  "fund_id": 1
}
```

**请求示例**:
```bash
curl -X POST http://localhost:8000/api/v1/user/favorite-funds \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_access_token" \
  -d '{"fund_id": 1}'
```

**响应示例**:
```json
{
  "status": "success",
  "message": "基金已添加到自选",
  "data": {
    "id": 1,
    "user_id": 1,
    "fund_id": 1,
    "fund_code": "000001",
    "fund_name": "易方达蓝筹精选混合",
    "short_name": "易方达蓝筹精选",
    "created_at": "2025-12-01T00:00:00"
  }
}
```

#### DELETE /api/v1/user/favorite-funds/{favorite_id}

**功能描述**: 从自选列表中删除基金

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| favorite_id | int | 是 | 自选基金ID |

**请求示例**:
```bash
curl -X DELETE http://localhost:8000/api/v1/user/favorite-funds/1 \
  -H "Authorization: Bearer your_access_token"
```

**响应示例**:
```json
{
  "status": "success",
  "message": "基金已从自选列表中删除"
}
```

#### GET /api/v1/user/fund-holdings

**功能描述**: 获取用户的基金持有记录

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页大小 |

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/user/fund-holdings \
  -H "Authorization: Bearer your_access_token"
```

**响应示例**:
```json
{
  "total": 3,
  "page": 1,
  "page_size": 10,
  "data": [
    {
      "id": 1,
      "user_id": 1,
      "fund_id": 1,
      "fund_code": "000001",
      "fund_name": "易方达蓝筹精选混合",
      "short_name": "易方达蓝筹精选",
      "shares": 1000.0,
      "cost_price": 2.5678,
      "total_cost": 2567.8,
      "current_value": 3000.0,
      "profit_loss": 432.2,
      "profit_loss_rate": 16.83,
      "created_at": "2025-12-01T00:00:00",
      "updated_at": "2025-12-01T00:00:00"
    }
  ]
}
```

#### POST /api/v1/user/transactions

**功能描述**: 添加基金交易记录

**请求体**:
```json
{
  "fund_id": 1,
  "transaction_type": "purchase",
  "amount": 1000.0,
  "price": 2.5678,
  "shares": 389.44,
  "fee": 5.0
}
```

**请求示例**:
```bash
curl -X POST http://localhost:8000/api/v1/user/transactions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer your_access_token" \
  -d '{"fund_id": 1, "transaction_type": "purchase", "amount": 1000.0, "price": 2.5678, "shares": 389.44, "fee": 5.0}'
```

**响应示例**:
```json
{
  "status": "success",
  "message": "交易记录添加成功",
  "data": {
    "id": 1,
    "user_id": 1,
    "fund_id": 1,
    "transaction_type": "purchase",
    "amount": 1000.0,
    "price": 2.5678,
    "shares": 389.44,
    "fee": 5.0,
    "transaction_date": "2025-12-01T00:00:00",
    "created_at": "2025-12-01T00:00:00"
  }
}
```

#### GET /api/v1/user/transactions

**功能描述**: 获取用户的交易记录

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页大小 |
| fund_id | int | 否 | None | 基金ID |
| transaction_type | string | 否 | None | 交易类型，如 purchase, redemption |
| start_date | string | 否 | None | 开始日期 |
| end_date | string | 否 | None | 结束日期 |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/user/transactions?page=1&page_size=10&transaction_type=purchase" \
  -H "Authorization: Bearer your_access_token"
```

**响应示例**:
```json
{
  "total": 5,
  "page": 1,
  "page_size": 10,
  "data": [
    {
      "id": 1,
      "user_id": 1,
      "fund_id": 1,
      "fund_code": "000001",
      "fund_name": "易方达蓝筹精选混合",
      "short_name": "易方达蓝筹精选",
      "transaction_type": "purchase",
      "amount": 1000.0,
      "price": 2.5678,
      "shares": 389.44,
      "fee": 5.0,
      "transaction_date": "2025-12-01T00:00:00",
      "created_at": "2025-12-01T00:00:00"
    }
  ]
}
```

### 8. 指数数据接口

#### GET /api/v1/index/list

**功能描述**: 获取指数列表

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 10 | 每页大小 |
| market | string | 否 | None | 市场，如 sh, sz, hs |
| index_type | string | 否 | None | 指数类型 |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/index/list?page=1&page_size=10&market=sh"
```

**响应示例**:
```json
{
  "total": 100,
  "page": 1,
  "page_size": 10,
  "data": [
    {
      "id": 1,
      "index_code": "000001",
      "index_name": "上证指数",
      "market": "sh",
      "index_type": "综合指数",
      "base_point": 100.0,
      "base_date": "1990-12-19",
      "created_at": "2025-12-01T00:00:00",
      "updated_at": "2025-12-01T00:00:00"
    }
  ]
}
```

#### GET /api/v1/index/history

**功能描述**: 获取指数历史数据

**请求参数**:
| 参数名 | 类型 | 必需 | 默认值 | 描述 |
|-------|------|------|--------|------|
| index_code | string | 是 | None | 指数代码 |
| start_date | string | 否 | None | 开始日期 |
| end_date | string | 否 | None | 结束日期 |
| page | int | 否 | 1 | 页码 |
| page_size | int | 否 | 100 | 每页大小 |

**请求示例**:
```bash
curl -X GET "http://localhost:8000/api/v1/index/history?index_code=000001&start_date=2025-01-01&end_date=2025-12-01"
```

**响应示例**:
```json
{
  "total": 240,
  "page": 1,
  "page_size": 100,
  "index_code": "000001",
  "index_name": "上证指数",
  "data": [
    {
      "date": "2025-12-01",
      "open": 3200.0,
      "high": 3250.0,
      "low": 3190.0,
      "close": 3240.0,
      "volume": 1000000000,
      "amount": 15000000000.0
    }
  ]
}
```

#### GET /api/v1/index/info/{index_name}

**功能描述**: 获取指数详情

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| index_name | string | 是 | 指数名称 |

**请求示例**:
```bash
curl -X GET http://localhost:8000/api/v1/index/info/上证指数
```

**响应示例**:
```json
{
  "status": "success",
  "data": {
    "index_code": "000001",
    "index_name": "上证指数",
    "market": "sh",
    "index_type": "综合指数",
    "base_point": 100.0,
    "base_date": "1990-12-19",
    "latest_point": 3240.0,
    "change": 40.0,
    "change_rate": 1.25,
    "volume": 1000000000,
    "amount": 15000000000.0,
    "created_at": "2025-12-01T00:00:00",
    "updated_at": "2025-12-01T00:00:00"
  }
}
```

#### POST /api/v1/index/sync

**功能描述**: 同步指数数据

**请求参数**:
| 参数名 | 类型 | 必需 | 描述 |
|-------|------|------|------|
| index_code | string | 是 | 指数代码 |
| start_date | string | 否 | None | 开始日期 |
| end_date | string | 否 | None | 结束日期 |

**请求示例**:
```bash
curl -X POST "http://localhost:8000/api/v1/index/sync?index_code=000001&start_date=2025-01-01&end_date=2025-12-01" \
  -H "Authorization: Bearer your_access_token"
```

**响应示例**:
```json
{
  "status": "success",
  "message": "指数数据同步成功",
  "data": {
    "index_code": "000001",
    "start_date": "2025-01-01",
    "end_date": "2025-12-01",
    "synced_count": 240
  }
}
```

**查询参数**:
- `page`: 页码，默认 1
- `page_size`: 每页条数，默认 10
- `fund_code`: 基金代码
- `fund_name`: 基金名称（支持模糊查询）
- `fund_type`: 基金类型
- `company_id`: 基金公司ID
- `company_name`: 基金公司名称（支持模糊查询）
- `is_purchaseable`: 是否可购买
- `sort_by`: 排序字段
- `sort_order`: 排序方式，asc 或 desc

**响应示例**:
```json
{
  "total": 1000,
  "page": 1,
  "page_size": 10,
  "total_pages": 100,
  "data": [
    {
      "id": 1,
      "fund_code": "000001",
      "short_name": "华夏成长",
      "fund_name": "华夏成长混合",
      "fund_type": 1,
      "pinyin": "hxcz",
      "manager": "张三",
      "company_id": 1,
      "company_name": "华夏基金管理有限公司",
      "launch_date": "2001-12-18T00:00:00",
      "latest_nav": 1.2345,
      "latest_nav_date": "2025-12-28T00:00:00",
      "is_purchaseable": true,
      "purchase_min_amount": 10.0,
      "redemption_min_amount": 10.0,
      "risk_level": 3.0,
      "created_at": "2025-12-01T00:00:00",
      "updated_at": "2025-12-28T00:00:00"
    }
    // 更多基金数据...
  ]
}
```

### 4. 指数数据接口

#### GET /api/v1/index/history

**查询参数**:
- `index_name`: 指数名称（必填），如 "沪深300", "中证500", "中证1000", "创业板指", "科创50"
- `start_date`: 开始日期，格式 YYYY-MM-DD
- `end_date`: 结束日期，格式 YYYY-MM-DD

**响应示例**:
```json
[
  {
    "date": "2025-12-01",
    "open": 4539.19,
    "close": 4576.49,
    "high": 4576.97,
    "low": 4531.29,
    "volume": 200382317,
    "amount": 463861587251.4
  },
  {
    "date": "2025-12-02",
    "open": 4571.91,
    "close": 4554.33,
    "high": 4575.49,
    "low": 4539.43,
    "volume": 148493132,
    "amount": 364366426477.8
  }
  // 更多历史数据...
]
```

## 数据模型

### 基金基本信息

| 字段名 | 类型 | 描述 |
|-------|------|------|
| id | Integer | 主键ID |
| fund_code | String | 基金代码 |
| short_name | String | 基金简称 |
| fund_name | String | 基金全称 |
| fund_type | Integer | 基金类型 |
| manager | String | 基金经理 |
| company_id | Integer | 基金公司ID |
| company_name | String | 基金公司名称 |
| launch_date | DateTime | 成立日期 |
| latest_nav | Float | 最新净值 |
| latest_nav_date | DateTime | 最新净值日期 |
| is_purchaseable | Boolean | 是否可购买 |
| risk_level | Float | 风险等级 |

### 基金历史数据

| 字段名 | 类型 | 描述 |
|-------|------|------|
| id | Integer | 主键ID |
| fund_id | Integer | 基金ID |
| trade_date | DateTime | 交易日期 |
| nav | Float | 单位净值 |
| accum_nav | Float | 累计净值 |
| daily_growth | Float | 日增长率 |

### 指数信息

| 字段名 | 类型 | 描述 |
|-------|------|------|
| id | Integer | 主键ID |
| index_name | String | 指数名称 |
| index_code | String | 指数代码 |
| secid | String | 东方财富secid |
| market | String | 市场 |
| description | Text | 指数描述 |

### 指数历史数据

| 字段名 | 类型 | 描述 |
|-------|------|------|
| id | Integer | 主键ID |
| index_id | Integer | 指数ID |
| trade_date | DateTime | 交易日期 |
| open | Float | 开盘价 |
| close | Float | 收盘价 |
| high | Float | 最高价 |
| low | Float | 最低价 |
| volume | Integer | 成交量 |
| amount | Float | 成交额 |

## 错误码说明

| 错误码 | 描述 |
|-------|------|
| 200 | 请求成功 |
| 400 | 请求参数错误 |
| 401 | 未授权，认证失败 |
| 403 | 禁止访问，权限不足 |
| 404 | 资源不存在 |
| 500 | 服务器内部错误 |

## 使用示例

### 1. 用户登录

```bash
curl -X POST "http://localhost:8000/api/v1/user/login" -H "Content-Type: application/json" -d '{"username":"admin","password":"password123"}'
```

### 2. 获取基金列表

```bash
curl -X GET "http://localhost:8000/api/v1/fund/?page=1&page_size=10&token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
```

### 3. 获取指数历史数据

```bash
curl -X GET "http://localhost:8000/api/v1/index/history?index_name=沪深300&start_date=2025-12-01&end_date=2025-12-10&token=eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9..."
```

## 注意事项

1. 所有需要认证的接口必须在请求中携带有效的 `token`
2. 分页接口默认返回 10 条数据，最大支持返回 1000 条
3. 模糊查询使用 `ilike` 操作符，不区分大小写
4. 时间字段使用 ISO 8601 格式（如：2025-12-29T15:00:00）
5. 金额和数值字段使用 Float 类型，保留两位小数

## 版本更新记录

### v1.0.0 (2025-12-29)

- 初始版本
- 实现基金数据采集和查询功能
- 实现用户管理和认证
- 实现基金交易模拟
- 实现指数历史数据爬取和查询

## 开发团队

- 开发人员：XXX
- 联系方式：XXX
- 文档更新时间：2025-12-29
