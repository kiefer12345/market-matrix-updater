"""
市场数据矩阵自动更新脚本 v8

数据口径由后端定死, 每个资产在 data.json 里自带:
  value_type  price / yield / spread / index / ratio
  change_type percent / absolute
  unit        % / pp / points / ratio / USD / USD/bbl
  source      实际使用的数据源; source_fallback=True 表示用了备用源
  as_of       该序列最新数据日期
前端只负责渲染, 不再根据资产名字猜单位。

数据源:
  Yahoo  ETF / 指数 / BTC / VIX
  FRED   2Y(DGS2) 10Y(DGS10, Yahoo ^TNX 备用) 高收益债利差 WTI 现货(DCOILWTICO)
  Cboe   Total Put/Call Ratio(Daily Market Statistics, 按日期取)

运行规则:
  - 必须配置 FRED_API_KEY, 否则直接失败(FRED 是核心数据源)
  - FRED API 失败才退到 CSV, 并标记 source_fallback
  - 最新数据超过阈值天数视为过期, 丢弃并在 missing 里写明原因
  - 成功资产不足 60% 时不覆盖 data.json、不写 Notion, 非零退出
  - DRY_RUN=1: 照常抓数和生成 data.json, 但不写 Notion(给 PR 试跑用)
"""

import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from io import StringIO

import pandas as pd
import requests
import yfinance as yf

# ============== 配置区域 ==============
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
FRED_API_KEY = os.environ.get("FRED_API_KEY")
DRY_RUN = os.environ.get("DRY_RUN") == "1"
DATABASE_ID = "1131248983354d15aec2933c5210bbdc"

DEFAULT_MAX_STALE_DAYS = 6
MIN_SUCCESS_RATIO = 0.6

# 口径模板
PRICE = dict(value_type="price", change_type="percent", unit="USD")
INDEX = dict(value_type="index", change_type="percent", unit="points")
YIELD = dict(value_type="yield", change_type="absolute", unit="pp")
SPREAD = dict(value_type="spread", change_type="absolute", unit="pp", invert_color=True)
VOL = dict(value_type="index", change_type="absolute", unit="points", invert_color=True)
RATIO = dict(value_type="ratio", change_type="absolute", unit="ratio", invert_color=True)

# 资产配置: key 同时是 Notion 页面标题, 不要随便改; 显示名用 label
ASSETS = {
    # ---- Yahoo ----
    "美元":         dict(INDEX, label="美元指数", yahoo="DX-Y.NYB"),
    "TLT":          dict(PRICE, label="TLT", yahoo="TLT"),
    "标普500":      dict(PRICE, label="标普500 (SPY)", yahoo="SPY"),
    "纳指":         dict(PRICE, label="纳指100 (QQQ)", yahoo="QQQ"),
    "道指":         dict(PRICE, label="道指 (DIA)", yahoo="DIA"),
    "罗素2000":     dict(PRICE, label="罗素2000 (IWM)", yahoo="IWM"),
    "标普等权指数": dict(PRICE, label="标普等权 (RSP)", yahoo="RSP"),
    "罗素1000价值": dict(PRICE, label="罗素1000价值 (IWD)", yahoo="IWD"),
    "罗素1000成长": dict(PRICE, label="罗素1000成长 (IWF)", yahoo="IWF"),
    "前7大科技":    dict(PRICE, label="前7大科技 (MAGS)", yahoo="MAGS"),
    "趋势板块":     dict(PRICE, label="动量因子 (MTUM)", yahoo="MTUM"),
    "黄金":         dict(PRICE, label="黄金 (GLD)", yahoo="GLD"),
    "比特币":       dict(PRICE, label="比特币", yahoo="BTC-USD"),
    "科技":         dict(PRICE, label="科技 (XLK)", yahoo="XLK"),
    "芯片":         dict(PRICE, label="芯片 (SOXX)", yahoo="SOXX"),
    "能源":         dict(PRICE, label="能源 (XLE)", yahoo="XLE"),
    "银行":         dict(PRICE, label="银行 (KBWB)", yahoo="KBWB"),
    "保险板块":     dict(PRICE, label="保险 (IAK)", yahoo="IAK"),
    "医疗":         dict(PRICE, label="医疗 (XLV)", yahoo="XLV"),
    "通讯服务":     dict(PRICE, label="通讯服务 (XLC)", yahoo="XLC"),
    "非必需品消费": dict(PRICE, label="非必需消费 (XLY)", yahoo="XLY"),
    "必需品消费":   dict(PRICE, label="必需消费 (XLP)", yahoo="XLP"),
    "公共事业":     dict(PRICE, label="公用事业 (XLU)", yahoo="XLU"),
    "REITS":        dict(PRICE, label="REITs (IYR)", yahoo="IYR"),
    "VIX":          dict(VOL, label="VIX", yahoo="^VIX"),
    # ---- FRED ----
    "2年美债":      dict(YIELD, label="2年美债", fred="DGS2"),
    "10年美债":     dict(YIELD, label="10年美债", fred="DGS10", yahoo_fallback="^TNX"),
    "垃圾债券利差": dict(SPREAD, label="高收益债利差 (OAS)", fred="BAMLH0A0HYM2"),
    # EIA 现货价发布有滞后, 放宽过期阈值
    "WTI原油":      dict(PRICE, label="WTI 原油现货", unit="USD/bbl",
                         fred="DCOILWTICO", max_stale_days=12),
    # ---- Cboe ----
    "PUT/CALL":     dict(RATIO, label="Total Put/Call Ratio", cboe="TOTAL PUT/CALL RATIO"),
}

META_KEYS = ("label", "value_type", "change_type", "unit", "invert_color")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Accept": "application/json,text/csv,text/plain,*/*;q=0.8",
}

LOOKBACKS = {
    "1星期": pd.DateOffset(days=7),
    "1个月": pd.DateOffset(months=1),
    "1年": pd.DateOffset(years=1),
    "3年": pd.DateOffset(years=3),
    "5年": pd.DateOffset(years=5),
}
PERIOD_KEYS = ["1天", "1星期", "1个月", "QTD", "YTD", "1年", "3年", "5年"]

missing = {}  # key -> {"label", "reason"}


def today_utc():
    return pd.Timestamp.now(tz="UTC").tz_localize(None).normalize()


def safe_float(value):
    if value is None:
        return None
    try:
        value = float(value)
    except (TypeError, ValueError):
        return None
    if math.isnan(value) or math.isinf(value):
        return None
    return round(value, 6)


def mark_missing(key, reason):
    missing[key] = {"label": ASSETS[key]["label"], "reason": reason}
    print(f"✗ {ASSETS[key]['label']}: {reason}")


# ---------------- 计算 ----------------

def base_on_or_before(close, when):
    """when 当天或之前最后一个值; 历史不够长返回 None"""
    if close.index[0] > when:
        return None
    return close.loc[:when].iloc[-1]


def period_targets(last_date):
    """各周期的基准目标日期(实际取该日或之前最近一个有数据的日子)"""
    q_start = pd.Timestamp(last_date.year, ((last_date.month - 1) // 3) * 3 + 1, 1)
    y_start = pd.Timestamp(last_date.year, 1, 1)
    targets = {k: last_date - off for k, off in LOOKBACKS.items()}
    targets["QTD"] = q_start - pd.Timedelta(days=1)   # 上季末
    targets["YTD"] = y_start - pd.Timedelta(days=1)   # 上年末
    return targets


def compute_changes(close, change_type):
    close = close.dropna()
    close = close[~close.index.duplicated(keep="last")].sort_index()
    if len(close) < 2:
        return None
    last_date, last = close.index[-1], float(close.iloc[-1])

    def change(base):
        if base is None:
            return None
        base = float(base)
        if change_type == "percent":
            # 基准 <= 0 时百分比无意义(如 2020-04 WTI 负油价)
            return None if base <= 0 else safe_float(last / base - 1)
        return safe_float(last - base)

    out = {"收盘价": safe_float(last), "as_of": last_date.strftime("%Y-%m-%d")}
    out["1天"] = change(close.iloc[-2])
    for k, when in period_targets(last_date).items():
        out[k] = change(base_on_or_before(close, when))
    return {k: v for k, v in out.items() if v is not None}


def add_asset(all_data, key, series, source, fallback=False):
    cfg = ASSETS[key]
    if series is None or len(series.dropna()) < 2:
        mark_missing(key, f"{source}: 无数据")
        return False
    series = series.dropna()
    if series.index.tz is not None:
        series.index = series.index.tz_localize(None)
    last = series.index[-1]
    age = (today_utc() - last.normalize()).days
    limit = cfg.get("max_stale_days", DEFAULT_MAX_STALE_DAYS)
    if age > limit:
        mark_missing(key, f"{source}: 数据过期, 最新 {last:%Y-%m-%d}({age} 天前, 阈值 {limit} 天)")
        return False
    result = compute_changes(series, cfg["change_type"])
    if not result:
        mark_missing(key, f"{source}: 计算失败")
        return False
    result.update({m: cfg[m] for m in META_KEYS if m in cfg})
    result["source"] = source
    result["source_fallback"] = fallback
    all_data[key] = result
    missing.pop(key, None)
    tag = " [备用源]" if fallback else ""
    print(f"✓ {cfg['label']}: {result['收盘价']} @ {result['as_of']} ({source}){tag}")
    return True


# ---------------- Yahoo ----------------

def fetch_yahoo(tickers):
    out = {}
    try:
        df = yf.download(list(tickers), period="6y", progress=False,
                         auto_adjust=True, group_by="column", threads=True)
    except Exception as e:
        print(f"  Yahoo 批量下载失败: {e}")
        df = None
    if df is not None and not df.empty and isinstance(df.columns, pd.MultiIndex):
        close = df["Close"]
        for t in tickers:
            if t in close.columns:
                out[t] = close[t].dropna()
    for t in tickers:  # 批量里缺的逐个重试
        if t in out and len(out[t]) >= 2:
            continue
        try:
            d = yf.download(t, period="6y", progress=False, auto_adjust=True)
            if d is not None and not d.empty:
                c = d["Close"]
                out[t] = (c[t] if isinstance(c, pd.DataFrame) else c).dropna()
        except Exception as e:
            print(f"  {t} 重试失败: {e}")
        time.sleep(0.2)
    return out


# ---------------- FRED ----------------

def fred_api(series_id):
    r = requests.get(
        "https://api.stlouisfed.org/fred/series/observations",
        params={"series_id": series_id, "api_key": FRED_API_KEY,
                "file_type": "json", "observation_start": "2019-01-01"},
        timeout=30,
    )
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}: {r.text[:120]}")
    obs = r.json().get("observations", [])
    df = pd.DataFrame(obs)
    s = pd.to_numeric(df["value"], errors="coerce")
    s.index = pd.to_datetime(df["date"])
    return s.dropna().sort_index()


def fred_csv(series_id):
    r = requests.get("https://fred.stlouisfed.org/graph/fredgraph.csv",
                     params={"id": series_id, "cosd": "2019-01-01"},
                     headers=HEADERS, timeout=30)
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    df = pd.read_csv(StringIO(r.text))
    s = pd.to_numeric(df.iloc[:, 1], errors="coerce")
    s.index = pd.to_datetime(df.iloc[:, 0])
    return s.dropna().sort_index()


def fetch_fred(series_id):
    """返回 (series, source, fallback)"""
    for attempt in range(2):
        try:
            return fred_api(series_id), f"FRED {series_id}", False
        except Exception as e:
            print(f"  FRED API {series_id} 第{attempt + 1}次失败: {e}")
            time.sleep(2)
    try:
        return fred_csv(series_id), f"FRED {series_id} (CSV)", True
    except Exception as e:
        print(f"  FRED CSV {series_id} 失败: {e}")
    return None, f"FRED {series_id}", False


# ---------------- Cboe ----------------

CBOE_URL = "https://cdn.cboe.com/data/us/options/market_statistics/daily/{d}_daily_options"
_cboe_cache = {}


def cboe_ratio_on(date, field):
    """某日的比率; 非交易日 Cboe 返回 403/404 -> None"""
    d = date.strftime("%Y-%m-%d")
    if d not in _cboe_cache:
        val = None
        try:
            r = requests.get(CBOE_URL.format(d=d), headers=HEADERS, timeout=20)
            if r.status_code == 200:
                for item in r.json().get("ratios", []):
                    if item.get("name", "").strip().upper() == field:
                        val = safe_float(item.get("value"))
            elif r.status_code not in (403, 404):
                print(f"  Cboe {d}: HTTP {r.status_code}")
        except Exception as e:
            print(f"  Cboe {d}: {e}")
        _cboe_cache[d] = val
    return _cboe_cache[d]


def cboe_on_or_before(target, field, max_back=7):
    for i in range(max_back + 1):
        day = target - pd.Timedelta(days=i)
        if day.weekday() >= 5:
            continue
        v = cboe_ratio_on(day, field)
        if v is not None:
            return day, v
    return None, None


def fetch_cboe(field):
    """只取需要的日期: 最新、前一交易日、各周期基准日"""
    latest_date, latest = cboe_on_or_before(today_utc(), field)
    if latest_date is None:
        return None
    points = {latest_date: latest}
    prev_date, prev = cboe_on_or_before(latest_date - pd.Timedelta(days=1), field)
    if prev_date is not None:
        points[prev_date] = prev
    for when in period_targets(latest_date).values():
        d, v = cboe_on_or_before(when, field)
        if d is not None:
            points[d] = v
    return pd.Series(points).sort_index()


# ---------------- 主流程 ----------------

def fetch_all_data():
    all_data = {}

    print("\n--- Yahoo Finance ---")
    tickers = {c[k] for c in ASSETS.values() for k in ("yahoo", "yahoo_fallback") if k in c}
    prices = fetch_yahoo(sorted(tickers))
    for key, cfg in ASSETS.items():
        if "yahoo" in cfg:
            add_asset(all_data, key, prices.get(cfg["yahoo"]), f"Yahoo {cfg['yahoo']}")

    print("\n--- FRED ---")
    for key, cfg in ASSETS.items():
        if "fred" not in cfg:
            continue
        s, source, fallback = fetch_fred(cfg["fred"])
        if add_asset(all_data, key, s, source, fallback):
            continue
        if "yahoo_fallback" in cfg:
            t = cfg["yahoo_fallback"]
            add_asset(all_data, key, prices.get(t), f"Yahoo {t}", fallback=True)

    print("\n--- Cboe ---")
    for key, cfg in ASSETS.items():
        if "cboe" in cfg:
            add_asset(all_data, key, fetch_cboe(cfg["cboe"]), "Cboe Daily Market Statistics")

    return all_data


def print_report(market_data):
    """核对表: 逐项和原始数据源抽查用"""
    print("\n" + "=" * 110)
    print(f"{'资产':<22}{'水平':>12}  {'as_of':<11}{'1W':>9}{'QTD':>9}{'YTD':>9}{'1Y':>9}{'5Y':>9}  来源")
    print("-" * 110)

    def fmt(v, ct):
        if v is None:
            return "-"
        return f"{v * 100:+.2f}%" if ct == "percent" else f"{v:+.2f}"

    for key, cfg in ASSETS.items():
        a = market_data.get(key)
        if not a:
            print(f"{cfg['label']:<22}{'缺失':>12}  {missing.get(key, {}).get('reason', '')}")
            continue
        ct = a["change_type"]
        cols = "".join(f"{fmt(a.get(p), ct):>9}" for p in ["1星期", "QTD", "YTD", "1年", "5年"])
        fb = " [备用]" if a["source_fallback"] else ""
        print(f"{cfg['label']:<22}{a['收盘价']:>12.4f}  {a['as_of']:<11}{cols}  {a['source']}{fb}")
    print("=" * 110)


# ---------------- Notion ----------------

def notion_headers():
    return {"Authorization": f"Bearer {NOTION_API_KEY}",
            "Notion-Version": "2022-06-28", "Content-Type": "application/json"}


def notion_request(method, url, **kw):
    r = None
    for attempt in range(3):
        r = requests.request(method, url, headers=notion_headers(), timeout=30, **kw)
        if r.status_code == 429 or r.status_code >= 500:
            time.sleep(float(r.headers.get("Retry-After", 2 ** attempt)))
            continue
        return r
    return r


def get_notion_pages():
    pages, cursor = [], None
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    while True:
        body = {"page_size": 100}
        if cursor:
            body["start_cursor"] = cursor
        r = notion_request("POST", url, json=body)
        if r.status_code != 200:
            print(f"获取 Notion 页面失败: {r.status_code} - {r.text[:300]}")
            return pages
        data = r.json()
        pages += data["results"]
        if not data.get("has_more"):
            return pages
        cursor = data["next_cursor"]


def update_notion_page(page_id, data):
    props = {f: {"number": data[f]}
             for f in ["收盘价", "1天", "1星期", "1个月", "1年", "QTD", "YTD"]
             if data.get(f) is not None}
    props["更新时间"] = {"date": {"start": data["as_of"]}}
    r = notion_request("PATCH", f"https://api.notion.com/v1/pages/{page_id}",
                       json={"properties": props})
    if r.status_code != 200:
        print(f"    {r.status_code}: {r.text[:200]}")
    return r.status_code == 200


def update_notion_database(market_data):
    if DRY_RUN:
        print("DRY_RUN: 跳过 Notion")
        return
    if not NOTION_API_KEY:
        print("跳过 Notion 更新(无 API Key)")
        return
    pages = get_notion_pages()
    if not pages:
        print("警告: 未获取到 Notion 页面")
        return
    for page in pages:
        title = page["properties"].get("资产名称", {}).get("title") or []
        name = title[0]["plain_text"] if title else None
        if name in market_data:
            ok = update_notion_page(page["id"], market_data[name])
            print(f"{'✓' if ok else '✗'} 更新 {name}")
            time.sleep(0.35)


# ---------------- 输出 ----------------

def save_json_data(market_data):
    output = {
        "schema_version": 2,
        "updateTime": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "periods": PERIOD_KEYS,
        "assets": market_data,
        "missing": missing,
    }
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    print("✓ 已保存 data.json")


def main():
    print("=" * 50)
    print(f"市场数据矩阵更新器 v8  {datetime.now(timezone.utc):%Y-%m-%d %H:%M UTC}"
          + ("  [DRY_RUN]" if DRY_RUN else ""))
    print("=" * 50)

    if not FRED_API_KEY:
        print("✗ 未配置 FRED_API_KEY。2Y、10Y、高收益债利差、WTI 都依赖 FRED, 请在仓库 Secrets 里添加。")
        sys.exit(2)

    market_data = fetch_all_data()
    print_report(market_data)
    print(f"\n成功 {len(market_data)}/{len(ASSETS)}")

    if len(market_data) < len(ASSETS) * MIN_SUCCESS_RATIO:
        print("✗ 成功资产过少, 保留旧 data.json, 不更新 Notion")
        sys.exit(1)

    save_json_data(market_data)
    update_notion_database(market_data)
    print("更新完成")


if __name__ == "__main__":
    main()
