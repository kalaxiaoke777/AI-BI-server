from datetime import datetime
import requests


def get_fund_valuation(fund_code):
    url = f"https://m.dayfund.cn/ajs/ajaxdata.shtml?showtype=getfundvalue&fundcode={fund_code}"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Referer": "https://fund.eastmoney.com/",
    }  # 加headers防反爬

    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # 检查请求是否成功
        source = response.text.split("|")
        data = {
            "fund_code": fund_code,
            "yes_date": source[0],
            "yes_value": source[1],
            "yes_subtract": source[3],
            "yes_reduct": source[4],
            "day_date": source[10],
            "day_value": source[7],
            "day_subtract": source[6],
            "day_reduct": source[5],
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        return data

    except Exception as e:
        return {"error": str(e)}


# 示例：实时轮询（每60秒一次，按Ctrl+C停止）
fund_code = "020594"  # 替换成你的基金代码
data = get_fund_valuation(fund_code)
print(data)
