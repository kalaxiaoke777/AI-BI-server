import requests
from typing import List, Dict, Any
from loguru import logger
from bs4 import BeautifulSoup
from app.scrapers.base import BaseScraper, RawData, DataType, DataSource
import re
import concurrent.futures
import time
from threading import Lock


class EastMoneyScraper(BaseScraper):
    """东方财富基金数据爬虫"""

    def __init__(self):
        super().__init__(DataSource.EASTMONEY)
        self.base_url = "https://fund.eastmoney.com"
        self.api_url = "https://api.fund.eastmoney.com"
        self.fund_list_url = "https://fund.eastmoney.com/js/fundcode_search.js"
        self.rank_api_url = "https://fund.eastmoney.com/data/rankhandler.aspx"

        # 线程池配置
        self.max_workers = 5  # 最大并发数
        self.request_interval = 2  # 请求间隔，单位：秒
        self.lock = Lock()  # 锁，用于控制请求间隔
        self.last_request_time = 0  # 上次请求时间

    def _wait_for_request(self):
        """等待请求间隔，避免频繁请求"""
        with self.lock:
            current_time = time.time()
            time_since_last_request = current_time - self.last_request_time
            if time_since_last_request < self.request_interval:
                time.sleep(self.request_interval - time_since_last_request)
            self.last_request_time = time.time()

    def _get_fund_rank_page(self, page: int, page_size: int = 50) -> Dict[str, Any]:
        """获取单页基金排行数据

        Args:
            page: 页码
            page_size: 每页大小

        Returns:
            Dict[str, Any]: 基金排行数据
        """
        self.logger.info(f"获取基金排行数据，页码: {page}")

        try:
            # 构建请求URL
            params = {
                "op": "ph",
                "dt": "kf",
                "ft": "all",
                "rs": "",
                "gs": 0,
                "sc": "dm",
                "st": "asc",
                "sd": "2024-12-25",
                "ed": "2025-12-25",
                "qdii": "",
                "tabSubtype": ",,,,,",
                "pi": page,
                "pn": page_size,
                "dx": 1,
                "v": str(time.time()),  # 使用时间戳作为动态参数
            }

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                "Referer": f"{self.base_url}/data/fundranking.html",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br, zstd",
                "Accept-Language": "zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6",
                "Connection": "keep-alive",
            }

            # 等待请求间隔
            self._wait_for_request()

            # 发送请求
            response = requests.get(
                self.rank_api_url, params=params, headers=headers, timeout=10
            )
            response.raise_for_status()

            # 解析响应数据
            content = response.text

            # 提取基金排行数据
            # 格式示例：var rankData = {datas:["000001,华夏成长混合,HXCZHH,2025-12-24,1.076,3.6...", ...]} 或 var rankData ={ErrCode:-999,Data:"无访问权限"}

            # 匹配rankData变量
            rank_data_match = re.search(
                r"var rankData = (\{.*?\});", content, re.DOTALL
            )

            if not rank_data_match:
                self.logger.error(f"未找到rankData变量，页码: {page}")
                return {"data": [], "total": 0}

            try:
                # 解析JSON数据
                rank_data_str = rank_data_match.group(1)

                # 特殊处理：东方财富返回的datas字段是字符串数组，而非嵌套数组
                # 示例：{datas:["000001,华夏成长混合,HXCZHH,2025-12-24,1.076,3.6...", ...]}

                # 替换单引号为双引号，确保JSON格式正确
                import re

                # 匹配datas字段的内容
                datas_pattern = r"datas:\[(.*?)\]"
                datas_match = re.search(datas_pattern, rank_data_str, re.DOTALL)

                if datas_match:
                    # 提取datas数组内容
                    datas_content = datas_match.group(1)
                    # 将datas内容替换为标准JSON格式
                    # 首先将字符串数组转换为实际数组
                    # 例如："000001,华夏成长混合,HXCZHH,2025-12-24,1.076,3.6..." → ["000001", "华夏成长混合", "HXCZHH", "2025-12-24", "1.076", "3.6..."]

                    # 提取所有基金字符串
                    fund_strings = re.findall(r'"([^"]+)"', datas_content)

                    # 解析每个基金字符串
                    result = []
                    for fund_str in fund_strings:
                        # 分割逗号分隔的字段
                        fund_fields = fund_str.split(",")
                        if len(fund_fields) >= 10:
                            # 提取需要的字段
                            result.append(
                                {
                                    "fund_code": fund_fields[0],
                                    "fund_name": fund_fields[1],
                                    "short_name": fund_fields[2],
                                    "nav_date": fund_fields[3],
                                    "nav": (
                                        float(fund_fields[4])
                                        if fund_fields[4] != ""
                                        else None
                                    ),
                                    "accum_nav": (
                                        float(fund_fields[5])
                                        if fund_fields[5] != ""
                                        else None
                                    ),
                                    "daily_growth": (
                                        float(fund_fields[6].rstrip("%"))
                                        if fund_fields[6] != ""
                                        else None
                                    ),
                                    "weekly_growth": (
                                        float(fund_fields[7].rstrip("%"))
                                        if fund_fields[7] != ""
                                        else None
                                    ),
                                    "monthly_growth": (
                                        float(fund_fields[8].rstrip("%"))
                                        if fund_fields[8] != ""
                                        else None
                                    ),
                                    "quarterly_growth": (
                                        float(fund_fields[9].rstrip("%"))
                                        if fund_fields[9] != ""
                                        else None
                                    ),
                                    "yearly_growth": (
                                        float(fund_fields[10].rstrip("%"))
                                        if len(fund_fields) > 10
                                        and fund_fields[10] != ""
                                        else None
                                    ),
                                }
                            )

                    # 提取总数量
                    total_count_pattern = r"totalCount:(\d+)"
                    total_count_match = re.search(total_count_pattern, rank_data_str)
                    total_count = (
                        int(total_count_match.group(1))
                        if total_count_match
                        else len(result)
                    )

                    self.logger.info(
                        f"获取基金排行数据成功，页码: {page}，数据量: {len(result)}"
                    )
                    return {"data": result, "total": total_count}
                else:
                    self.logger.error(f"未找到datas字段，页码: {page}")
                    return {"data": [], "total": 0}

            except Exception as e:
                self.logger.error(
                    f"获取基金排行数据失败，解析错误，页码: {page}，错误: {str(e)}"
                )
                import traceback

                traceback.print_exc()
                return {"data": [], "total": 0}

        except requests.RequestException as e:
            self.logger.error(
                f"获取基金排行数据失败，网络请求错误，页码: {page}，错误: {str(e)}"
            )
        except Exception as e:
            self.logger.error(
                f"获取基金排行数据失败，解析错误，页码: {page}，错误: {str(e)}"
            )

        return {"data": [], "total": 0}

    def get_all_fund_rank_data(self, max_pages: int = None) -> List[Dict[str, Any]]:
        """获取所有基金排行数据

        Args:
            max_pages: 最大页码，为None时获取所有数据，默认为None

        Returns:
            List[Dict[str, Any]]: 基金排行数据列表
        """
        self.logger.info(f"开始获取所有基金排行数据，max_pages: {max_pages}")

        result = []
        total = 0

        try:
            # 如果没有设置max_pages，直接一次性获取所有数据
            if max_pages is None:
                self.logger.info("直接获取所有基金排行数据")
                # 设置一个很大的page_size，一次性获取所有数据
                all_data = self._get_fund_rank_page(page=1, page_size=100000)  # 设置很大的page_size
                result.extend(all_data.get("data", []))
                total = all_data.get("total", len(result))
                self.logger.info(f"一次性获取所有数据成功，共 {len(result)} 条记录")
            else:
                # 分页获取数据
                self.logger.info(f"分页获取基金排行数据，max_pages: {max_pages}")
                
                # 获取第一页数据，了解总页数
                first_page = self._get_fund_rank_page(page=1)
                result.extend(first_page.get("data", []))
                total = first_page.get("total", 0)

                if total == 0:
                    self.logger.error("获取总页数失败")
                    return result

                # 计算实际需要爬取的总页数
                actual_total_pages = (total + 49) // 50  # 每页50条数据

                # 如果max_pages大于实际总页数，则获取所有数据
                if max_pages > actual_total_pages:
                    total_pages = actual_total_pages
                else:
                    total_pages = max_pages

                self.logger.info(
                    f"总数据量: {total}，总页数: {actual_total_pages}，计划爬取页数: {total_pages}"
                )

                # 使用线程池获取剩余页码数据
                if total_pages > 1:
                    with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
                        # 生成页码列表
                        pages = list(range(2, total_pages + 1))

                        # 提交任务
                        future_to_page = {
                            executor.submit(self._get_fund_rank_page, page): page for page in pages
                        }

                        # 处理结果
                        for future in concurrent.futures.as_completed(future_to_page):
                            page = future_to_page[future]
                            try:
                                page_data = future.result()
                                result.extend(page_data.get("data", []))
                                self.logger.info(
                                    f"成功获取页码 {page} 的数据，新增 {len(page_data.get('data', []))} 条记录"
                                )
                            except Exception as e:
                                self.logger.error(f"处理页码 {page} 数据失败，错误: {str(e)}")
        except Exception as e:
            self.logger.error(f"获取所有基金排行数据失败，错误: {str(e)}")

        self.logger.info(f"获取所有基金排行数据完成，共 {len(result)} 条数据")
        return result

    def get_fund_company_list(self) -> List[Dict[str, Any]]:
        """获取基金公司列表

        Returns:
            List[Dict[str, Any]]: 基金公司列表
        """
        self.logger.info("开始获取基金公司列表")

        try:
            # 使用用户提供的新接口
            company_url = "https://fund.eastmoney.com/Data/FundRankScale.aspx"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
                "Referer": self.base_url,
            }

            # 等待请求间隔
            self._wait_for_request()

            response = requests.get(company_url, headers=headers, timeout=10)
            response.raise_for_status()

            # 解析响应数据
            content = response.text
            self.logger.info(f"获取到响应内容，长度: {len(content)}")
            
            # 直接使用正则表达式提取所有公司数据
            # 匹配所有 ['10001055', '国海证券股份有限公司', ...] 格式的数据
            # 注意：使用更简单的模式，匹配整个数组元素
            import re
            # 匹配整个公司数据数组元素，包括所有单引号包裹的字段
            company_pattern = re.compile(r"\['([^']+(?:'[^']*')*[^']+[^']*)\]")
            matches = company_pattern.findall(content)
            
            company_list = []
            if matches:
                self.logger.info(f"使用正则表达式提取到 {len(matches)} 条原始记录")
                
                # 处理每个匹配到的公司数据
                for match in matches:
                    try:
                        # 将匹配结果转换为Python列表
                        # 格式：'10001055','国海证券股份有限公司','1993-06-28','','度万中','GHZQ','','44.75','★★★★','国海证券','','2025/9/30 0:00:00'
                        # 添加括号并使用ast.literal_eval()解析
                        company_str = f"['{match}']"
                        company_data = eval(company_str)  # 使用eval直接解析
                        
                        # 提取需要的字段
                        if len(company_data) >= 10:
                            company_list.append({
                                "company_code": company_data[0],
                                "company_name": company_data[1],
                                "established_date": company_data[2],
                                "fund_count": company_data[3],
                                "manager": company_data[4],
                                "pinyin": company_data[5],
                                "asset_scale": company_data[7],
                                "rating": company_data[8],
                                "short_name": company_data[9]
                            })
                    except Exception as e:
                        self.logger.error(f"处理公司数据失败: {str(e)}")
                        self.logger.info(f"原始记录: {match}")
            else:
                self.logger.error("未匹配到任何公司数据")
                self.logger.info(f"响应内容前1000字符: {content[:1000]}")
                return []

            self.logger.info(f"获取基金公司列表成功，共 {len(company_list)} 个公司")
            return company_list

        except requests.RequestException as e:
            self.logger.error(f"获取基金公司列表失败，网络请求错误: {str(e)}")
        except Exception as e:
            self.logger.error(f"获取基金公司列表失败，解析错误: {str(e)}")
            import traceback
            traceback.print_exc()

        return []

    def get_all_fund_codes(self) -> List[str]:
        """获取所有基金代码列表

        Returns:
            List[str]: 基金代码列表
        """
        fund_list = self.get_all_fund_data()
        return [fund["fund_code"] for fund in fund_list]

    def get_all_fund_data(self) -> List[Dict[str, Any]]:
        """获取所有基金的完整数据列表

        Returns:
            List[Dict[str, Any]]: 基金完整数据列表
        """
        self.logger.info("开始获取所有基金完整数据")

        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": self.base_url,
            }

            response = requests.get(self.fund_list_url, headers=headers, timeout=10)
            response.raise_for_status()

            # 解析返回的JavaScript代码
            content = response.text
            self.logger.debug(f"原始响应: {content[:100]}...")
            
            # 使用正则表达式提取完整数组
            # 东方财富返回格式: var r = [["000001","HXCZHH","华夏成长混合","混合型-偏股","HXCZHH"], [...]];
            import re
            array_pattern = r"var r = (\[.*?\]);"
            array_match = re.search(array_pattern, content, re.DOTALL)
            
            if not array_match:
                self.logger.error("未找到数组数据")
                return []
            
            # 提取完整数组字符串
            array_str = array_match.group(1)
            self.logger.debug(f"提取的完整数组: {array_str[:100]}...")
            
            # 使用ast.literal_eval()安全解析JavaScript数组
            import ast
            fund_data = ast.literal_eval(array_str)
            
            # 转换为结构化数据
            result = []
            for fund in fund_data:
                if len(fund) >= 5:
                    result.append(
                        {
                            "fund_code": fund[0],
                            "short_name": fund[1],
                            "fund_name": fund[2],
                            "fund_type": fund[3],
                            "pinyin": fund[4],
                        }
                    )

            self.logger.info(f"获取基金完整数据成功，共 {len(result)} 个基金")
            return result

        except requests.RequestException as e:
            self.logger.error(f"获取基金完整数据失败，网络请求错误: {str(e)}")
        except ast.literal_eval.MalformedExpressionError as e:
            self.logger.error(f"获取基金完整数据失败，解析错误: {str(e)}")
            self.logger.debug(f"原始响应: {content[:500]}")
        except Exception as e:
            self.logger.error(f"获取基金完整数据失败，解析错误: {str(e)}")
            self.logger.debug(f"原始响应: {content[:500]}")
            import traceback
            traceback.print_exc()

        return []

    def get_data_url(self, **kwargs) -> str:
        """获取东方财富基金数据的URL

        Args:
            kwargs: 构建URL的参数，包括fund_code（基金代码）、data_type（数据类型）等

        Returns:
            str: 数据URL
        """
        fund_code = kwargs.get("fund_code")
        data_type = kwargs.get("data_type", DataType.FUND_BASIC)

        if data_type == DataType.FUND_BASIC:
            # 基金基础信息URL
            return f"{self.base_url}/{fund_code}.html"
        elif data_type == DataType.FUND_DAILY:
            # 基金日线数据API
            return f"{self.api_url}/fund/nav"
        elif data_type == DataType.FUND_HOLDINGS:
            # 基金持仓数据URL
            return f"{self.base_url}/fund/f10/FundArchivesDatas.aspx?type=jjcc&code={fund_code}"
        else:
            # 其他类型数据默认使用基础信息URL
            return f"{self.base_url}/{fund_code}.html"

    def fetch_data(self, **kwargs) -> List[RawData]:
        """抓取东方财富基金数据

        Args:
            kwargs: 抓取参数，包括fund_code_list（基金代码列表）、data_type（数据类型）等

        Returns:
            List[RawData]: 抓取到的原始数据列表
        """
        fund_code_list = kwargs.get(
            "fund_code_list", ["000001"]
        )  # 默认抓取华夏成长混合
        data_type = kwargs.get("data_type", DataType.FUND_BASIC)

        raw_data_list = []

        for fund_code in fund_code_list:
            try:
                url = self.get_data_url(fund_code=fund_code, data_type=data_type)
                self.logger.info(f"开始抓取基金数据，基金代码: {fund_code}，URL: {url}")

                # 设置请求头
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Referer": self.base_url,
                }

                # 发送请求
                if data_type == DataType.FUND_DAILY:
                    # API 请求，需要参数
                    params = {
                        "fundCode": fund_code,
                        "pageIndex": 1,
                        "pageSize": 100,
                        "startDate": "",
                        "endDate": "",
                        "_": "1703500000000",
                    }
                    response = requests.get(
                        url, headers=headers, params=params, timeout=10
                    )
                else:
                    # 网页请求
                    response = requests.get(url, headers=headers, timeout=10)

                # 检查响应状态
                response.raise_for_status()

                # 创建 RawData 对象
                raw_data = RawData(
                    fund_code=fund_code,
                    data_type=data_type,
                    source=self.data_source,
                    source_url=url,
                    raw_content=response.text,
                    metadata={
                        "status_code": response.status_code,
                        "content_type": response.headers.get("Content-Type"),
                        "url": url,
                    },
                )

                raw_data_list.append(raw_data)
                self.logger.info(f"抓取成功，基金代码: {fund_code}")

            except requests.RequestException as e:
                self.logger.error(f"抓取失败，基金代码: {fund_code}，错误: {str(e)}")
            except Exception as e:
                self.logger.error(f"处理失败，基金代码: {fund_code}，错误: {str(e)}")

        return raw_data_list

    def parse_data(self, raw_content: str, **kwargs) -> Dict[str, Any]:
        """解析东方财富基金原始数据

        Args:
            raw_content: 原始数据内容
            kwargs: 解析参数，包括data_type（数据类型）等

        Returns:
            Dict[str, Any]: 结构化数据
        """
        data_type = kwargs.get("data_type", DataType.FUND_BASIC)

        try:
            if data_type == DataType.FUND_DAILY:
                # 解析API返回的JSON数据
                import json

                data = json.loads(raw_content)
                return data
            else:
                # 解析网页HTML数据
                from bs4 import BeautifulSoup

                soup = BeautifulSoup(raw_content, "html.parser")

                if data_type == DataType.FUND_BASIC:
                    # 解析基金基础信息
                    return self._parse_fund_basic(soup)
                elif data_type == DataType.FUND_HOLDINGS:
                    # 解析基金持仓信息
                    return self._parse_fund_holdings(raw_content)
                else:
                    # 其他类型数据默认返回HTML文本
                    return {"html": raw_content}

        except Exception as e:
            self.logger.error(f"解析失败，数据类型: {data_type}，错误: {str(e)}")
            return {"error": str(e), "raw_content": raw_content[:100]}

    def _parse_fund_basic(self, soup: BeautifulSoup) -> Dict[str, Any]:
        """解析基金基础信息

        Args:
            soup: BeautifulSoup对象

        Returns:
            Dict[str, Any]: 基金基础信息
        """
        fund_info = {}

        try:
            # 获取基金名称和代码
            name_element = soup.find("div", class_="fundDetail-tit")
            if name_element:
                fund_info["name"] = name_element.text.strip()

            # 获取基金类型
            type_element = soup.find("div", class_="infoOfFund").find("a")
            if type_element:
                fund_info["type"] = type_element.text.strip()

            # 获取最新净值
            nav_element = soup.find("div", class_="dataItem02").find(
                "span", class_="ui-font-large ui-color-red"
            )
            if nav_element:
                fund_info["latest_nav"] = nav_element.text.strip()

            # 获取净值日期
            nav_date_element = soup.find("div", class_="dataItem02").find("p")
            if nav_date_element:
                fund_info["nav_date"] = nav_date_element.text.strip().replace(
                    "最新净值日期：", ""
                )

        except Exception as e:
            self.logger.error(f"解析基金基础信息失败: {str(e)}")

        return fund_info

    def _parse_fund_holdings(self, raw_content: str) -> Dict[str, Any]:
        """解析基金持仓信息

        Args:
            raw_content: 原始HTML内容

        Returns:
            Dict[str, Any]: 基金持仓信息
        """
        holdings_info = {}

        try:
            # 东方财富基金持仓数据是通过JavaScript动态加载的，需要从HTML中提取JSON数据
            import re
            import json

            # 匹配持仓数据的正则表达式
            pattern = r"var apidata=\{(.*?)\};"
            match = re.search(pattern, raw_content, re.DOTALL)

            if match:
                json_str = "{" + match.group(1) + "}"
                data = json.loads(json_str)
                holdings_info = data

        except Exception as e:
            self.logger.error(f"解析基金持仓信息失败: {str(e)}")

        return holdings_info

    def get_fund_growth_data(self, fund_code: str) -> List[Dict[str, Any]]:
        """获取基金历史涨幅数据

        Args:
            fund_code: 基金代码

        Returns:
            List[Dict[str, Any]]: 基金历史涨幅数据列表
        """
        self.logger.info(f"开始获取基金历史涨幅数据，基金代码: {fund_code}")

        try:
            # 基金详情页URL
            detail_url = f"{self.base_url}/{fund_code}.html"

            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": self.base_url,
            }

            response = requests.get(detail_url, headers=headers, timeout=10)
            response.raise_for_status()

            # 解析HTML，获取涨幅数据
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(response.text, "html.parser")

            # 查找涨幅数据区域
            growth_data = []

            # 1. 查找近1日涨幅
            nav_element = soup.find("div", class_="dataItem02")
            if nav_element:
                # 查找涨幅百分比
                growth_element = nav_element.find(
                    "span", class_="ui-font-large ui-color-red"
                )
                if growth_element:
                    growth_data.append(
                        {
                            "growth_type": "近1日",
                            "growth_value": float(growth_element.text.strip()),
                        }
                    )

            # 2. 查找其他周期涨幅（近1周、近1月等）
            growth_section = soup.find("div", class_="dataItem04")
            if growth_section:
                # 查找涨幅数据项
                growth_items = growth_section.find_all("div", class_="dataItem04Item")
                for item in growth_items:
                    # 提取涨幅类型和值
                    label = item.find("span", class_="dataItem04ItemTitle").text.strip()
                    value = item.find("span", class_="dataItem04ItemVal").text.strip()

                    # 转换涨幅值为浮点数
                    if value.endswith("%"):
                        value = float(value[:-1])
                    else:
                        value = float(value)

                    growth_data.append({"growth_type": label, "growth_value": value})

            self.logger.info(
                f"获取基金历史涨幅数据成功，基金代码: {fund_code}，数据项: {len(growth_data)}"
            )
            return growth_data

        except requests.RequestException as e:
            self.logger.error(f"获取基金历史涨幅数据失败，网络请求错误: {str(e)}")
        except Exception as e:
            self.logger.error(f"获取基金历史涨幅数据失败，解析错误: {str(e)}")

        return []
