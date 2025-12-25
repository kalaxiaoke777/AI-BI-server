import requests
from typing import List, Dict, Any
from loguru import logger
from bs4 import BeautifulSoup
from app.scrapers.base import BaseScraper, RawData, DataType, DataSource
import re

class EastMoneyScraper(BaseScraper):
    """东方财富基金数据爬虫"""
    
    def __init__(self):
        super().__init__(DataSource.EASTMONEY)
        self.base_url = "https://fund.eastmoney.com"
        self.api_url = "https://api.fund.eastmoney.com"
        self.fund_list_url = "https://fund.eastmoney.com/js/fundcode_search.js"
    
    def get_all_fund_codes(self) -> List[str]:
        """获取所有基金代码列表
        
        Returns:
            List[str]: 基金代码列表
        """
        fund_list = self.get_all_fund_data()
        return [fund['fund_code'] for fund in fund_list]
    
    def get_all_fund_data(self) -> List[Dict[str, Any]]:
        """获取所有基金的完整数据列表
        
        Returns:
            List[Dict[str, Any]]: 基金完整数据列表
        """
        self.logger.info("开始获取所有基金完整数据")
        
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                "Referer": self.base_url
            }
            
            response = requests.get(self.fund_list_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            # 解析返回的JavaScript代码
            content = response.text
            # 提取JSON数组部分
            json_str = content.replace('var r = ', '').replace(';', '')
            
            # 解析JSON数据
            import json
            fund_data = json.loads(json_str)
            
            # 转换为结构化数据
            result = []
            for fund in fund_data:
                if len(fund) >= 5:
                    result.append({
                        'fund_code': fund[0],
                        'short_name': fund[1],
                        'fund_name': fund[2],
                        'fund_type': fund[3],
                        'pinyin': fund[4]
                    })
            
            self.logger.info(f"获取基金完整数据成功，共 {len(result)} 个基金")
            return result
        
        except requests.RequestException as e:
            self.logger.error(f"获取基金完整数据失败，网络请求错误: {str(e)}")
        except json.JSONDecodeError as e:
            self.logger.error(f"获取基金完整数据失败，JSON解析错误: {str(e)}")
        except Exception as e:
            self.logger.error(f"获取基金完整数据失败，解析错误: {str(e)}")
        
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
        fund_code_list = kwargs.get("fund_code_list", ["000001"])  # 默认抓取华夏成长混合
        data_type = kwargs.get("data_type", DataType.FUND_BASIC)
        
        raw_data_list = []
        
        for fund_code in fund_code_list:
            try:
                url = self.get_data_url(fund_code=fund_code, data_type=data_type)
                self.logger.info(f"开始抓取基金数据，基金代码: {fund_code}，URL: {url}")
                
                # 设置请求头
                headers = {
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                    "Referer": self.base_url
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
                        "_": "1703500000000"
                    }
                    response = requests.get(url, headers=headers, params=params, timeout=10)
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
                        "url": url
                    }
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
            nav_element = soup.find("div", class_="dataItem02").find("span", class_="ui-font-large ui-color-red")
            if nav_element:
                fund_info["latest_nav"] = nav_element.text.strip()
            
            # 获取净值日期
            nav_date_element = soup.find("div", class_="dataItem02").find("p")
            if nav_date_element:
                fund_info["nav_date"] = nav_date_element.text.strip().replace("最新净值日期：", "")
            
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
