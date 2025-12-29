import akshare as ak

# 上证指数 2019 至今（腾讯源）
df = ak.stock_zh_index_daily_tx(symbol="sh000001")
print(df.tail())
