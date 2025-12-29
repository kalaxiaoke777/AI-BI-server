#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
指数相关API接口
"""

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Dict, Any
from datetime import datetime
from db import get_db
from db.models import IndexInfo, IndexHistory
from app.scrapers.eastmoney_index import EastMoneyIndexScraper

router = APIRouter(
    tags=["index"],
    responses={404: {"description": "Not found"}},
)


@router.get("/list", response_model=List[Dict[str, Any]])
async def get_index_list(db: Session = Depends(get_db)):
    """
    获取指数列表
    """
    try:
        indices = db.query(IndexInfo).all()
        result = []
        for index in indices:
            result.append(
                {
                    "id": index.id,
                    "index_name": index.index_name,
                    "index_code": index.index_code,
                    "secid": index.secid,
                    "market": index.market,
                    "description": index.description,
                    "created_at": index.created_at,
                    "updated_at": index.updated_at,
                }
            )
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指数列表失败: {str(e)}")


@router.get("/history", response_model=List[Dict[str, Any]])
async def get_index_history(
    index_name: str = Query(..., description="指数名称"),
    start_date: str = Query(None, description="开始日期，格式：YYYY-MM-DD"),
    end_date: str = Query(None, description="结束日期，格式：YYYY-MM-DD"),
    db: Session = Depends(get_db),
):
    """
    获取指数历史数据
    """
    try:
        # 查找指数信息
        index = db.query(IndexInfo).filter_by(index_name=index_name).first()
        if not index:
            raise HTTPException(status_code=404, detail=f"未找到指数: {index_name}")

        # 构建查询条件
        query = db.query(IndexHistory).filter(IndexHistory.index_id == index.id)

        # 添加日期过滤
        if start_date:
            query = query.filter(
                IndexHistory.trade_date >= datetime.strptime(start_date, "%Y-%m-%d")
            )
        if end_date:
            query = query.filter(
                IndexHistory.trade_date <= datetime.strptime(end_date, "%Y-%m-%d")
            )

        # 按日期排序
        histories = query.order_by(IndexHistory.trade_date).all()

        # 转换结果
        result = []
        for history in histories:
            result.append(
                {
                    "date": history.trade_date.strftime("%Y-%m-%d"),
                    "open": history.open,
                    "close": history.close,
                    "high": history.high,
                    "low": history.low,
                    "volume": history.volume,
                    "amount": history.amount,
                }
            )

        return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指数历史数据失败: {str(e)}")


@router.get("/info/{index_name}", response_model=Dict[str, Any])
async def get_index_info(index_code: str, db: Session = Depends(get_db)):
    """
    获取指数详情
    """
    try:
        index = db.query(IndexInfo).filter_by(index_code=index_code).first()
        if not index:
            raise HTTPException(status_code=404, detail=f"未找到指数: {index_code}")

        return {
            "id": index.id,
            "index_name": index.index_name,
            "index_code": index.index_code,
            "secid": index.secid,
            "market": index.market,
            "description": index.description,
            "created_at": index.created_at,
            "updated_at": index.updated_at,
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指数详情失败: {str(e)}")


@router.post("/sync")
async def sync_index_history_data(
    index_name: str = Query(None, description="指数名称，如果为None则同步所有指数"),
    start_date: str = Query(None, description="开始日期，格式：YYYY-MM-DD"),
    end_date: str = Query(None, description="结束日期，格式：YYYY-MM-DD"),
):
    """
    同步指数历史数据
    """
    try:
        from script.sync_index_history import sync_index_history

        result = sync_index_history(index_name, start_date, end_date)
        if result:
            return {"message": "指数历史数据同步成功"}
        else:
            raise HTTPException(status_code=500, detail="指数历史数据同步失败")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"同步指数历史数据失败: {str(e)}")
