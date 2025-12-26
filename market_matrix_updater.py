"""
市场数据矩阵自动更新脚本 v5
修复: FRED CSV列名问题 + CBOE 403问题（添加User-Agent）
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import requests
import os
import time
import math
import json
import urllib.request

# ============== 配置区域 ==============
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
FRED_API_KEY = os.environ.get("FRED_API_KEY")  # 可选
DATABASE_ID = "1131248983354d15aec2933c5210bbdc"

# 资产代码映射 - Yahoo Finance
TICKER_MAP = {
    "美元": "DX-Y.NYB",
    "2年美债": "^IRX",
    "10年美债": "^TNX",
    "TLT": "TLT",
    "标普500": "SPY",
    "纳指": "QQQ",
    "道指": "DIA",
    "黄金": "GLD",
    "WTI原油": "USO",
    "VIX": "^VIX",
    "罗素2000": "IWM",
    "比特币": "BTC-USD",
    "通讯服务": "IXP",
    "非必需品消费": "XLY",
    "必需品消费": "XLP",
    "能源": "XLE",
    "银行": "KBWB",
    "公共事业": "XLU",
    "REITS": "IYR",
    "科技": "XLK",
    "医疗": "XLV",
    "趋势板块": "MTUM",
    "保险板块": "IAK",
    "芯片": "SOXX",
    "罗素1000价值": "IWD",
    "罗素1000成长": "IWF",
    "前7大科技": "MAGS",
    "标普等权指数": "RSP",
}

def safe_float(value):
    """确保浮点数是JSON兼容的"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if math.isnan(value) or math.isinf(value):
            return None
        return round(float(value), 6)
    return None

def get_fred_data(series_id, api_key=None):
    """从FRED获取数据（垃圾债券利差等）"""
    try:
        # 方法1: 使用FRED API（推荐，更稳定）
        if api_key:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                "series_id": series_id,
                "api_key": api_key,
                "file_type": "json",
                "sort_order": "desc",
                "limit": 365
            }
            response = requests.get(url, params=params, timeout=30)
            if response.status_code == 200:
                data = response.json()
                observations = data.get("observations", [])
                if observations:
                    df = pd.DataFrame(observations)
                    df['date'] = pd.to_datetime(df['date'])
                    df['value'] = pd.to_numeric(df['value'], errors='coerce')
                    df = df.set_index('date').sort_index()
                    return df['value'].dropna()
        
        # 方法2: 直接从FRED网站下载CSV（无需API Key）
        url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"
        
        # 添加User-Agent避免被拒绝
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))
            
            # 自动检测列名（可能是DATE或其他）
            date_col = df.columns[0]  # 第一列是日期
            value_col = df.columns[1]  # 第二列是值
            
            df[date_col] = pd.to_datetime(df[date_col])
            df[value_col] = pd.to_numeric(df[value_col], errors='coerce')
            df = df.set_index(date_col).sort_index()
            
            return df[value_col].dropna()
        else:
            print(f"  FRED HTTP错误: {response.status_code}")
            return None
        
    except Exception as e:
        print(f"  FRED数据获取失败 ({series_id}): {e}")
        return None

def get_cboe_put_call_ratio():
    """从CBOE获取Put/Call Ratio"""
    try:
        url = "https://www.cboe.com/publish/ScheduledTask/MktData/datahouse/totalpc.csv"
        
        # 添加完整的浏览器Headers避免403
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            from io import StringIO
            # 跳过前2行注释
            df = pd.read_csv(StringIO(response.text), skiprows=2)
            
            # 自动检测日期列
            date_col = df.columns[0]
            df[date_col] = pd.to_datetime(df[date_col])
            df = df.set_index(date_col).sort_index()
            
            # 查找Put/Call列
            for col in df.columns:
                if 'P/C' in col.upper() or 'RATIO' in col.upper():
                    return pd.to_numeric(df[col], errors='coerce').dropna()
            
            # 如果没找到，用最后一列（通常是总比率）
            return pd.to_numeric(df.iloc[:, -1], errors='coerce').dropna()
        else:
            print(f"  CBOE HTTP错误: {response.status_code}")
            
            # 备用方案：使用urllib
            req = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(req, timeout=30) as resp:
                content = resp.read().decode('utf-8')
                from io import StringIO
                df = pd.read_csv(StringIO(content), skiprows=2)
                date_col = df.columns[0]
                df[date_col] = pd.to_datetime(df[date_col])
                df = df.set_index(date_col).sort_index()
                return pd.to_numeric(df.iloc[:, -1], errors='coerce').dropna()
                
    except Exception as e:
        print(f"  CBOE Put/Call数据获取失败: {e}")
        return None

def calculate_returns(df, ticker=None):
    """计算各时间周期的收益率"""
    if df is None or (hasattr(df, 'empty') and df.empty):
        return None
    
    try:
        # 处理Series或DataFrame
        if isinstance(df, pd.DataFrame):
            if isinstance(df.columns, pd.MultiIndex):
                close = df[('Close', ticker)] if ticker and ('Close', ticker) in df.columns else df['Close']
            else:
                close = df['Close'] if 'Close' in df.columns else df.iloc[:, 0]
        else:
            close = df  # 已经是Series
        
        close = close.dropna()
        if len(close) < 2:
            return None
            
        today = close.index[-1]
        close_price = float(close.iloc[-1])
        
        returns = {"收盘价": safe_float(close_price)}
        
        # 1天
        if len(close) >= 2:
            ret = (close.iloc[-1] / close.iloc[-2] - 1)
            returns["1天"] = safe_float(ret)
        
        # 1星期
        if len(close) >= 6:
            ret = (close.iloc[-1] / close.iloc[-6] - 1)
            returns["1星期"] = safe_float(ret)
        
        # 1个月
        if len(close) >= 22:
            ret = (close.iloc[-1] / close.iloc[-22] - 1)
            returns["1个月"] = safe_float(ret)
        
        # 1年
        if len(close) >= 253:
            ret = (close.iloc[-1] / close.iloc[-253] - 1)
            returns["1年"] = safe_float(ret)
        
        # QTD
        quarter_month = ((today.month - 1) // 3) * 3 + 1
        quarter_start = pd.Timestamp(datetime(today.year, quarter_month, 1))
        qtd_data = close[close.index >= quarter_start]
        if len(qtd_data) >= 2:
            ret = (qtd_data.iloc[-1] / qtd_data.iloc[0] - 1)
            returns["QTD"] = safe_float(ret)
        
        # YTD
        year_start = pd.Timestamp(datetime(today.year, 1, 1))
        ytd_data = close[close.index >= year_start]
        if len(ytd_data) >= 2:
            ret = (ytd_data.iloc[-1] / ytd_data.iloc[0] - 1)
            returns["YTD"] = safe_float(ret)
        
        return returns
    except Exception as e:
        print(f"  计算收益率错误: {e}")
        return None

def calculate_spread_changes(series):
    """计算利差/比率的变化（绝对值变化，不是百分比）"""
    if series is None or len(series) < 2:
        return None
    
    try:
        series = series.dropna()
        current = float(series.iloc[-1])
        
        result = {"收盘价": safe_float(current)}
        
        # 1天变化
        if len(series) >= 2:
            result["1天"] = safe_float(series.iloc[-1] - series.iloc[-2])
        
        # 1星期变化
        if len(series) >= 6:
            result["1星期"] = safe_float(series.iloc[-1] - series.iloc[-6])
        
        # 1个月变化
        if len(series) >= 22:
            result["1个月"] = safe_float(series.iloc[-1] - series.iloc[-22])
        
        # 1年变化
        if len(series) >= 253:
            result["1年"] = safe_float(series.iloc[-1] - series.iloc[-253])
        
        return result
    except Exception as e:
        print(f"  计算变化错误: {e}")
        return None

def fetch_all_data():
    """获取所有资产数据"""
    all_data = {}
    
    # 1. 获取Yahoo Finance数据
    print("\n--- Yahoo Finance 数据 ---")
    for name, ticker in TICKER_MAP.items():
        if ticker is None:
            continue
        try:
            df = yf.download(ticker, period="2y", progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                returns = calculate_returns(df, ticker)
                if returns and returns.get("收盘价"):
                    all_data[name] = returns
                    print(f"✓ {name} ({ticker}): {returns.get('收盘价'):.2f}")
                else:
                    print(f"✗ {name} ({ticker}): 无有效数据")
            else:
                print(f"✗ {name} ({ticker}): 下载失败")
        except Exception as e:
            print(f"✗ {name} ({ticker}): {e}")
        time.sleep(0.1)
    
    # 2. 获取垃圾债券利差 (FRED)
    print("\n--- FRED 数据 ---")
    try:
        hy_spread = get_fred_data("BAMLH0A0HYM2", FRED_API_KEY)
        if hy_spread is not None and len(hy_spread) > 0:
            spread_data = calculate_spread_changes(hy_spread)
            if spread_data:
                all_data["垃圾债券利差"] = spread_data
                print(f"✓ 垃圾债券利差: {spread_data.get('收盘价'):.2f}%")
            else:
                print("✗ 垃圾债券利差: 计算失败")
        else:
            print("✗ 垃圾债券利差: 无数据")
    except Exception as e:
        print(f"✗ 垃圾债券利差: {e}")
    
    # 3. 获取Put/Call Ratio (CBOE)
    print("\n--- CBOE 数据 ---")
    try:
        pc_ratio = get_cboe_put_call_ratio()
        if pc_ratio is not None and len(pc_ratio) > 0:
            pc_data = calculate_spread_changes(pc_ratio)
            if pc_data:
                all_data["PUT/CALL"] = pc_data
                print(f"✓ PUT/CALL Ratio: {pc_data.get('收盘价'):.2f}")
            else:
                print("✗ PUT/CALL Ratio: 计算失败")
        else:
            print("✗ PUT/CALL Ratio: 无数据")
    except Exception as e:
        print(f"✗ PUT/CALL Ratio: {e}")
    
    return all_data

def get_notion_pages():
    """获取Notion数据库中的所有页面"""
    url = f"https://api.notion.com/v1/databases/{DATABASE_ID}/query"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    response = requests.post(url, headers=headers, json={})
    if response.status_code == 200:
        return response.json()["results"]
    else:
        print(f"获取Notion页面失败: {response.status_code} - {response.text}")
        return []

def update_notion_page(page_id, data):
    """更新单个Notion页面"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    properties = {}
    
    if data.get("收盘价") is not None:
        properties["收盘价"] = {"number": data["收盘价"]}
    if data.get("1天") is not None:
        properties["1天"] = {"number": data["1天"]}
    if data.get("1星期") is not None:
        properties["1星期"] = {"number": data["1星期"]}
    if data.get("1个月") is not None:
        properties["1个月"] = {"number": data["1个月"]}
    if data.get("1年") is not None:
        properties["1年"] = {"number": data["1年"]}
    if data.get("QTD") is not None:
        properties["QTD"] = {"number": data["QTD"]}
    if data.get("YTD") is not None:
        properties["YTD"] = {"number": data["YTD"]}
    
    properties["更新时间"] = {
        "date": {"start": datetime.now().strftime("%Y-%m-%d")}
    }
    
    payload = {"properties": properties}
    
    response = requests.patch(url, headers=headers, json=payload)
    return response.status_code == 200

def update_notion_database(market_data):
    """更新整个Notion数据库"""
    if not NOTION_API_KEY:
        print("跳过Notion更新 (无API Key)")
        return
        
    pages = get_notion_pages()
    
    if not pages:
        print("警告: 未获取到Notion页面")
        return
    
    print("\n--- 更新Notion ---")
    for page in pages:
        title_prop = page["properties"].get("资产名称", {})
        if title_prop.get("title") and len(title_prop["title"]) > 0:
            name = title_prop["title"][0]["plain_text"]
            
            if name in market_data:
                success = update_notion_page(page["id"], market_data[name])
                status = "✓" if success else "✗"
                print(f"{status} 更新 {name}")
                time.sleep(0.35)

def save_json_data(market_data):
    """保存数据为JSON文件"""
    output = {
        "updateTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "assets": market_data
    }
    
    with open("data.json", "w", encoding="utf-8") as f:
        json.dump(output, f, ensure_ascii=False, indent=2)
    
    print("✓ 已保存 data.json")

def main():
    print("=" * 50)
    print("市场数据矩阵更新器 v5")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("修复: FRED列名 + CBOE User-Agent")
    print("=" * 50)
    
    print("\n[1/3] 获取市场数据...")
    market_data = fetch_all_data()
    print(f"\n成功获取 {len(market_data)} 个资产数据")
    
    print("\n[2/3] 保存JSON数据文件...")
    save_json_data(market_data)
    
    print("\n[3/3] 更新Notion数据库...")
    update_notion_database(market_data)
    
    print("\n" + "=" * 50)
    print("更新完成!")
    print("=" * 50)

if __name__ == "__main__":
    main()
