import time
from datetime import datetime
import akshare as ak
import pandas as pd

index_mapping = {
    "沪深300": {
        "index_code": "000300",
        "secid": "1.000300",
        "market": "沪市",
        "description": "沪深300指数...",
    },
    "中证500": {
        "index_code": "000905",
        "secid": "1.000905",
        "market": "沪市",
        "description": "中证500指数...",
    },
    "中证1000": {
        "index_code": "000852",
        "secid": "1.000852",
        "market": "沪市",
        "description": "中证1000指数...",
    },
    "创业板指": {
        "index_code": "399006",
        "secid": "0.399006",
        "market": "深市",
        "description": "创业板指数...",
    },
    "科创50": {
        "index_code": "000688",
        "secid": "1.000688",
        "market": "沪市",
        "description": "科创板50指数...",
    },
}

TARGET_CODES = {v["index_code"] for v in index_mapping.values()}


def _pick_index_spot_func():
    """
    从当前 akshare 版本中，挑一个可用的“指数实时行情（全量）”函数。
    不同版本函数名可能不同，因此做兼容探测。
    """
    candidates = [
        "stock_zh_index_spot",
        "stock_zh_index_spot_em",
        "stock_zh_index_spot_sina",
        "stock_zh_index_spot_tx",
        "index_zh_a_spot",  # 有些版本可能使用不同命名
        "index_zh_spot",
    ]
    for name in candidates:
        fn = getattr(ak, name, None)
        if callable(fn):
            return name, fn

    # 给出更可读的提示：列出可能相关的函数名
    related = sorted(
        n
        for n in dir(ak)
        if ("index" in n.lower() and ("spot" in n.lower() or "real" in n.lower()))
    )
    raise AttributeError(
        "当前 akshare 版本找不到指数实时行情接口（如 stock_zh_index_spot）。\n"
        f"可疑候选函数：{related[:80]}{' ...' if len(related) > 80 else ''}\n"
        "你可以把 `pip show akshare` 的版本号发我，我按该版本固定到正确函数名。"
    )


def fetch_indexes_snapshot() -> pd.DataFrame:
    """
    拉取全部指数实时行情，然后过滤出我们关心的几个指数。
    """
    func_name, spot_func = _pick_index_spot_func()
    df = spot_func()
    if df is None or df.empty:
        return pd.DataFrame()

    # 常见列名兼容（不同数据源/版本返回字段可能不同）
    code_candidates = ["代码", "index_code", "指数代码", "symbol", "代码编号"]
    name_candidates = ["名称", "name", "指数名称", "symbol_name"]

    code_col = next((c for c in code_candidates if c in df.columns), None)
    name_col = next((c for c in name_candidates if c in df.columns), None)

    if not code_col:
        raise RuntimeError(
            f"接口 {func_name} 未找到指数代码列，当前返回列: {list(df.columns)}"
        )

    df = df[df[code_col].astype(str).isin(TARGET_CODES)].copy()

    # 补齐本地名称 / 抓取时间
    code_to_name = {v["index_code"]: k for k, v in index_mapping.items()}
    df["本地名称"] = df[code_col].astype(str).map(code_to_name)
    if name_col and "名称" not in df.columns:
        df["名称"] = df[name_col]

    df["抓取时间"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    df["数据源函数"] = func_name
    return df


def loop_pull(
    interval_seconds: int = 10,
    save_csv: bool = False,
    csv_path: str = "index_snapshot.csv",
):
    while True:
        try:
            df = fetch_indexes_snapshot()
            if df.empty:
                print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 无数据/接口返回空")
            else:
                # 只打印部分关键列（若存在）
                prefer_cols = [
                    "抓取时间",
                    "本地名称",
                    "名称",
                    "代码",
                    "最新价",
                    "涨跌幅",
                    "涨跌额",
                    "成交额",
                    "数据源函数",
                ]
                cols = [c for c in prefer_cols if c in df.columns]
                print(
                    df[cols].to_string(index=False)
                    if cols
                    else df.to_string(index=False)
                )

                if save_csv:
                    header = not pd.io.common.file_exists(csv_path)
                    df.to_csv(
                        csv_path,
                        mode="a",
                        index=False,
                        header=header,
                        encoding="utf-8-sig",
                    )
        except Exception as e:
            print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] 拉取失败: {e}")

        time.sleep(interval_seconds)


if __name__ == "__main__":
    loop_pull(interval_seconds=10, save_csv=False)
