"""
市场数据矩阵自动更新脚本 v2
Market Matrix Auto Updater for Notion
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import requests
import os
import time
import math

# ============== 配置区域 ==============
NOTION_API_KEY = os.environ.get("NOTION_API_KEY")
DATABASE_ID = "1131248983354d15aec2933c5210bbdc"

# 资产代码映射
TICKER_MAP = {
    "美元": "DX-Y.NYB",
    "2年美债": "^IRX",
    "10年美债": "^TNX",
    "TLT": "TLT",
    "PUT/CALL": None,
    "垃圾债券利差": None,
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

def calculate_returns(df, ticker):
    """计算各时间周期的收益率"""
    if df is None or df.empty:
        return None
    
    try:
        # 处理多级列索引
        if isinstance(df.columns, pd.MultiIndex):
            close = df[('Close', ticker)] if ('Close', ticker) in df.columns else df['Close']
        else:
            close = df['Close']
        
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

def fetch_all_data():
    """获取所有资产数据"""
    all_data = {}
    
    for name, ticker in TICKER_MAP.items():
        if ticker is None:
            print(f"⊘ {name} (无数据源)")
            continue
        try:
            df = yf.download(ticker, period="2y", progress=False, auto_adjust=True)
            if df is not None and not df.empty:
                returns = calculate_returns(df, ticker)
                if returns and returns.get("收盘价"):
                    all_data[name] = returns
                    print(f"✓ {name} ({ticker})")
                else:
                    print(f"✗ {name} ({ticker}): 无有效数据")
            else:
                print(f"✗ {name} ({ticker}): 下载失败")
        except Exception as e:
            print(f"✗ {name} ({ticker}): {e}")
        time.sleep(0.1)
    
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
    pages = get_notion_pages()
    
    if not pages:
        print("警告: 未获取到Notion页面，请检查API Key和数据库权限")
        return
    
    for page in pages:
        title_prop = page["properties"].get("资产名称", {})
        if title_prop.get("title") and len(title_prop["title"]) > 0:
            name = title_prop["title"][0]["plain_text"]
            
            if name in market_data:
                success = update_notion_page(page["id"], market_data[name])
                status = "✓" if success else "✗"
                print(f"{status} 更新 {name}")
                time.sleep(0.35)

def main():
    print("=" * 50)
    print("市场数据矩阵更新器 v2")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    if not NOTION_API_KEY:
        print("错误: NOTION_API_KEY 环境变量未设置")
        return
    
    print("\n[1/2] 获取市场数据...")
    market_data = fetch_all_data()
    print(f"\n成功获取 {len(market_data)} 个资产数据")
    
    print("\n[2/2] 更新Notion数据库...")
    update_notion_database(market_data)
    
    print("\n" + "=" * 50)
    print("更新完成!")
    print("=" * 50)

if __name__ == "__main__":
    main()
