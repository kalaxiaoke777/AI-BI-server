import requests
from bs4 import BeautifulSoup
import logging

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_company_funds_api():
    """测试基金公司基金列表API"""
    # 测试URL - 东海基金公司(gsid=80205268)
    url = 'https://fund.eastmoney.com/Company/home/KFSFundNet?gsid=80205268&fundType='
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
    }
    
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        logger.info(f"请求成功，状态码: {response.status_code}")
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # 打印页面标题
        title = soup.title.text if soup.title else '无标题'
        logger.info(f"页面标题: {title}")
        
        # 查找公司信息
        company_info = soup.find('div', class_='companyInfo')
        if company_info:
            logger.info("找到公司信息")
            logger.info(f"公司信息HTML: {company_info.text.strip()[:200]}...")
        
        # 查找基金表格
        # 注意：根据提供的网页参考，表格可能没有特定的class，让我们查找所有表格
        tables = soup.find_all('table')
        logger.info(f"找到 {len(tables)} 个表格")
        
        for i, table in enumerate(tables):
            rows = table.find_all('tr')
            logger.info(f"表格 {i+1} 有 {len(rows)} 行")
            
            if len(rows) > 1:
                logger.info(f"表格 {i+1} 第一行数据: {rows[0].text.strip()}")
                logger.info(f"表格 {i+1} 第二行数据: {rows[1].text.strip()[:200]}...")
                
                # 检查是否是基金列表表格
                if '基金名称' in rows[0].text and '代码' in rows[0].text:
                    logger.info(f"表格 {i+1} 是基金列表表格")
                    
                    # 解析基金数据
                    headers = [th.text.strip() for th in rows[0].find_all('th')]
                    logger.info(f"表头: {headers}")
                    
                    # 解析第一只基金数据
                    first_fund_row = rows[1]
                    cells = [td.text.strip() for td in first_fund_row.find_all('td')]
                    logger.info(f"第一只基金数据: {cells}")
                    break
        
    except requests.RequestException as e:
        logger.error(f"请求失败: {str(e)}")
    except Exception as e:
        logger.error(f"处理失败: {str(e)}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_company_funds_api()
