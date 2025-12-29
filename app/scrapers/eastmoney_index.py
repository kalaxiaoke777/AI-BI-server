import requests
import re
import json
from typing import List, Dict, Any
from loguru import logger
from datetime import datetime, timedelta


class EastMoneyIndexScraper:
    """东方财富指数历史数据爬虫"""

    def __init__(self):
        self.logger = logger.bind(source="eastmoney_index")
        self.base_url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
            "Referer": "http://quote.eastmoney.com/center/",
            "Accept": "*/*",
            "Accept-Encoding": "gzip, deflate",
            "Accept-Language": "zh-CN,zh;q=0.9",
            "Connection": "keep-alive"
        }

        # 指数代码映射（东方财富格式）
        self.index_codes = {
            "沪深300": "1.000300",  # 上证指数格式为1.XXXXXX
            "中证500": "1.000905",
            "中证1000": "1.000852",
            "创业板指": "0.399006",  # 深证指数格式为0.XXXXXX
            "科创50": "1.000688"
        }

    def get_index_history(self, index_name: str, start_date: str = None, end_date: str = None, klt: int = 101) -> List[Dict[str, Any]]:
        """获取指数历史数据

        Args:
            index_name: 指数名称
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            klt: 周期，101=日线，102=周线，103=月线，默认日线

        Returns:
            List[Dict[str, Any]]: 历史数据列表，每个元素为字典，包含日期、开盘价、收盘价、最高价、最低价、成交量、成交额
        """
        if index_name not in self.index_codes:
            self.logger.error(f"不支持的指数名称: {index_name}")
            return []

        secid = self.index_codes[index_name]
        self.logger.info(f"获取指数 {index_name} ({secid}) 的历史数据...")

        # 设置默认日期范围
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            # 默认获取最近5年数据
            start_date = (datetime.now() - timedelta(days=5*365)).strftime("%Y-%m-%d")

        # 构建请求参数
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": klt,
            "fqt": 0,  # 不复权
            "beg": start_date.replace("-", ""),  # 格式：YYYYMMDD
            "end": end_date.replace("-", ""),  # 格式：YYYYMMDD
            "_": int(datetime.now().timestamp() * 1000)
        }

        try:
            # 发送请求
            response = requests.get(self.base_url, params=params, headers=self.headers, timeout=15)
            response.raise_for_status()

            # 解析响应
            data = response.json()

            if data.get("rc") != 0:
                self.logger.error(f"获取失败，错误代码: {data.get('rc')}, 错误信息: {data.get('msg')}")
                return []

            # 解析数据
            klines = data.get("data", {}).get("klines", [])
            if not klines:
                self.logger.error("未返回数据")
                return []

            result = []
            for kline in klines:
                # 东方财富返回的数据格式："2025-12-25,3400.0000,3410.0000,3420.0000,3390.0000,10000000000,20000000000"
                # 对应：日期,开盘价,收盘价,最高价,最低价,成交量,成交额
                fields = kline.split(",")
                if len(fields) < 7:
                    continue

                result.append({
                    "date": fields[0],
                    "open": float(fields[1]),
                    "close": float(fields[2]),
                    "high": float(fields[3]),
                    "low": float(fields[4]),
                    "volume": int(float(fields[5])) if fields[5] else 0,  # 有些数据可能为空
                    "amount": float(fields[6]) if fields[6] else 0.0
                })

            self.logger.info(f"成功获取 {index_name} 指数 {len(result)} 条历史数据")
            return result

        except Exception as e:
            self.logger.error(f"获取指数历史数据失败: {str(e)}")
            import traceback
            traceback.print_exc()
            return []

    def get_all_index_history(self, start_date: str = None, end_date: str = None, klt: int = 101) -> Dict[str, List[Dict[str, Any]]]:
        """获取所有支持的指数历史数据

        Args:
            start_date: 开始日期，格式：YYYY-MM-DD
            end_date: 结束日期，格式：YYYY-MM-DD
            klt: 周期，101=日线，102=周线，103=月线，默认日线

        Returns:
            Dict[str, List[Dict[str, Any]]]: 所有指数的历史数据，键为指数名称，值为历史数据列表
        """
        self.logger.info(f"获取所有支持的指数历史数据...")
        result = {}

        for index_name in self.index_codes:
            history_data = self.get_index_history(index_name, start_date, end_date, klt)
            result[index_name] = history_data

        return result

    def get_index_list(self) -> List[str]:
        """获取支持的指数列表

        Returns:
            List[str]: 支持的指数名称列表
        """
        return list(self.index_codes.keys())
