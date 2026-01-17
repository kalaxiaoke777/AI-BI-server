from datetime import datetime
import requests
from bs4 import BeautifulSoup
import time


def get_fund_valuation(fund_code):
    url = f"https://fund.eastmoney.com/{fund_code}.html"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://fund.eastmoney.com/",
    }  # 加headers防反爬

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        soup = BeautifulSoup(response.text, "html.parser")

        # 查找估算净值部分（class="dataItem02"）
        valuation_div = soup.find("div", class_="dataOfFund")
        if not valuation_div:
            return {"error": "未找到估值模块，可能非交易时间或网页结构变"}
        valuation_div = valuation_div.find("dl", class_="dataItem01")

        # 估算净值 (gsz)
        gsz_elem = valuation_div.find("dd", class_="dataNums")
        gsz = gsz_elem.find("span").text.strip() if gsz_elem else "N/A"

        # 估算涨幅 (gszzl) 第二个span
        gszzl = (
            gsz_elem.find_all("span")[1].text.strip()
            if gsz_elem and len(gsz_elem.find_all("span")) > 1
            else "N/A"
        )

        return {
            "fund_code": fund_code,
            "gsz": gsz,  # 估算净值
            "gszzl": gszzl,  # 估算涨幅
            "gztime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),  # 估值时间
        }

    except Exception as e:
        return {"error": str(e)}


# 示例：实时轮询（每60秒一次，按Ctrl+C停止）
fund_code = "025499"  # 替换成你的基金代码
data = get_fund_valuation(fund_code)
print(data)
