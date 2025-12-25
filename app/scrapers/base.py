from abc import ABC, abstractmethod
from typing import List, Dict, Any
from dataclasses import dataclass
from enum import Enum
from loguru import logger

# 数据类型枚举
class DataType(str, Enum):
    FUND_BASIC = "fund_basic"  # 基金基础信息
    FUND_DAILY = "fund_daily"  # 基金日线数据
    FUND_HOLDINGS = "fund_holdings"  # 基金持仓
    FUND_RATING = "fund_rating"  # 基金评级
    OTHER = "other"  # 其他类型

# 数据来源枚举
class DataSource(str, Enum):
    EASTMONEY = "eastmoney"  # 东方财富
    TIANTIAN = "tiantian"  # 天天基金
    XUEQIU = "xueqiu"  # 雪球
    ANT = "ant"  # 蚂蚁财富
    OTHER = "other"  # 其他来源

# 原始数据模型
@dataclass
class RawData:
    fund_code: str  # 基金代码
    data_type: DataType  # 数据类型
    source: DataSource  # 数据来源
    source_url: str  # 数据来源URL
    raw_content: str  # 原始数据内容
    metadata: Dict[str, Any] = None  # 附加元数据
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}

# 抽象数据源接口
class BaseScraper(ABC):
    """数据源抽象基类，定义所有数据源必须实现的方法"""
    
    def __init__(self, data_source: DataSource):
        self.data_source = data_source
        self.logger = logger.bind(source=data_source.value)
    
    @abstractmethod
    def fetch_data(self, **kwargs) -> List[RawData]:
        """抓取数据的核心方法
        
        Args:
            kwargs: 抓取参数，如基金代码列表、日期范围等
            
        Returns:
            List[RawData]: 抓取到的原始数据列表
        """
        pass
    
    @abstractmethod
    def parse_data(self, raw_content: str, **kwargs) -> Dict[str, Any]:
        """解析原始数据为结构化数据
        
        Args:
            raw_content: 原始数据内容
            kwargs: 解析参数
            
        Returns:
            Dict[str, Any]: 结构化数据
        """
        pass
    
    @abstractmethod
    def get_data_url(self, **kwargs) -> str:
        """获取数据的URL
        
        Args:
            kwargs: 构建URL的参数
            
        Returns:
            str: 数据URL
        """
        pass
    
    def pre_process(self, **kwargs) -> Dict[str, Any]:
        """预处理，如设置请求头、代理等
        
        Args:
            kwargs: 预处理参数
            
        Returns:
            Dict[str, Any]: 预处理后的参数
        """
        self.logger.info(f"开始预处理，参数: {kwargs}")
        return kwargs
    
    def post_process(self, raw_data_list: List[RawData]) -> List[RawData]:
        """后处理，如数据清洗、去重等
        
        Args:
            raw_data_list: 原始数据列表
            
        Returns:
            List[RawData]: 后处理后的原始数据列表
        """
        self.logger.info(f"开始后处理，数据量: {len(raw_data_list)}")
        # 默认实现：去重
        seen = set()
        unique_data = []
        for data in raw_data_list:
            key = f"{data.fund_code}-{data.data_type}-{data.source}-{data.source_url}"
            if key not in seen:
                seen.add(key)
                unique_data.append(data)
        
        self.logger.info(f"后处理完成，去重前: {len(raw_data_list)}，去重后: {len(unique_data)}")
        return unique_data
    
    def run(self, **kwargs) -> List[RawData]:
        """完整的运行流程：预处理 -> 抓取 -> 后处理
        
        Args:
            kwargs: 运行参数
            
        Returns:
            List[RawData]: 最终的原始数据列表
        """
        self.logger.info(f"开始运行抓取任务，参数: {kwargs}")
        
        # 1. 预处理
        processed_kwargs = self.pre_process(**kwargs)
        
        # 2. 抓取数据
        raw_data_list = self.fetch_data(**processed_kwargs)
        
        # 3. 后处理
        final_data_list = self.post_process(raw_data_list)
        
        self.logger.info(f"抓取任务完成，最终数据量: {len(final_data_list)}")
        return final_data_list
