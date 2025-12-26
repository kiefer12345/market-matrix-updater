"""
市场数据矩阵自动更新脚本
Market Matrix Auto Updater for Notion

使用方法:
1. 安装依赖: pip install yfinance pandas requests python-dotenv
2. 创建 .env 文件并添加: NOTION_API_KEY=your_api_key
3. 运行: python market_matrix_updater.py

设置每日自动运行:
- Mac/Linux: 使用 crontab -e 添加定时任务
- Windows: 使用 Task Scheduler
- 云端: 使用 GitHub Actions (见下方说明)
"""

import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import time

# 加载环境变量
load_dotenv()

# ============== 配置区域 ==============
NOTION_API_KEY = os.getenv("NOTION_API_KEY")
DATABASE_ID = "1131248983354d15aec2933c5210bbdc"  # 你的Notion数据库ID

# 资产代码映射 (Notion名称 -> Yahoo Finance代码)
TICKER_MAP = {
    "美元": "DX-Y.NYB",           # 美元指数
    "2年美债": "^IRX",             # 2年国债收益率 (用13周作为近似)
    "10年美债": "^TNX",            # 10年国债收益率
    "TLT": "TLT",
    "PUT/CALL": None,              # 需要从CBOE获取，暂时跳过
    "垃圾债券利差": None,           # FRED数据，需要单独处理
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

# ============== 数据获取函数 ==============

def get_market_data(ticker: str, period: str = "2y") -> pd.DataFrame:
    """获取单个资产的历史数据"""
    try:
        data = yf.download(ticker, period=period, progress=False)
        return data
    except Exception as e:
        print(f"获取 {ticker} 数据失败: {e}")
        return None

def calculate_returns(df: pd.DataFrame) -> dict:
    """计算各时间周期的收益率"""
    if df is None or df.empty:
        return None
    
    today = df.index[-1]
    close_price = df['Close'].iloc[-1]
    
    # 计算各周期收益率
    returns = {"收盘价": float(close_price)}
    
    # 1天
    if len(df) >= 2:
        returns["1天"] = float((df['Close'].iloc[-1] / df['Close'].iloc[-2] - 1))
    
    # 1星期 (5个交易日)
    if len(df) >= 6:
        returns["1星期"] = float((df['Close'].iloc[-1] / df['Close'].iloc[-6] - 1))
    
    # 1个月 (约21个交易日)
    if len(df) >= 22:
        returns["1个月"] = float((df['Close'].iloc[-1] / df['Close'].iloc[-22] - 1))
    
    # 1年 (约252个交易日)
    if len(df) >= 253:
        returns["1年"] = float((df['Close'].iloc[-1] / df['Close'].iloc[-253] - 1))
    
    # QTD (季度至今)
    current_quarter_start = pd.Timestamp(datetime(today.year, ((today.month - 1) // 3) * 3 + 1, 1))
    qtd_data = df[df.index >= current_quarter_start]
    if len(qtd_data) >= 2:
        returns["QTD"] = float((qtd_data['Close'].iloc[-1] / qtd_data['Close'].iloc[0] - 1))
    
    # YTD (年度至今)
    year_start = pd.Timestamp(datetime(today.year, 1, 1))
    ytd_data = df[df.index >= year_start]
    if len(ytd_data) >= 2:
        returns["YTD"] = float((ytd_data['Close'].iloc[-1] / ytd_data['Close'].iloc[0] - 1))
    
    return returns

def fetch_all_data() -> dict:
    """获取所有资产数据"""
    all_data = {}
    
    # 获取所有需要的ticker
    tickers = [t for t in TICKER_MAP.values() if t is not None]
    
    print(f"正在下载 {len(tickers)} 个资产数据...")
    
    # 批量下载
    data = yf.download(tickers, period="2y", progress=True, group_by='ticker')
    
    for name, ticker in TICKER_MAP.items():
        if ticker is None:
            continue
        try:
            if len(tickers) > 1:
                ticker_data = data[ticker] if ticker in data.columns.get_level_values(0) else None
            else:
                ticker_data = data
            
            if ticker_data is not None and not ticker_data.empty:
                returns = calculate_returns(ticker_data)
                if returns:
                    all_data[name] = returns
                    print(f"✓ {name} ({ticker})")
        except Exception as e:
            print(f"✗ {name} ({ticker}): {e}")
    
    return all_data

# ============== Notion API 函数 ==============

def get_notion_pages() -> list:
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
        print(f"获取Notion页面失败: {response.text}")
        return []

def update_notion_page(page_id: str, data: dict):
    """更新单个Notion页面"""
    url = f"https://api.notion.com/v1/pages/{page_id}"
    headers = {
        "Authorization": f"Bearer {NOTION_API_KEY}",
        "Notion-Version": "2022-06-28",
        "Content-Type": "application/json"
    }
    
    # 构建properties
    properties = {}
    
    if "收盘价" in data:
        properties["收盘价"] = {"number": data["收盘价"]}
    if "1天" in data:
        properties["1天"] = {"number": data["1天"]}
    if "1星期" in data:
        properties["1星期"] = {"number": data["1星期"]}
    if "1个月" in data:
        properties["1个月"] = {"number": data["1个月"]}
    if "1年" in data:
        properties["1年"] = {"number": data["1年"]}
    if "QTD" in data:
        properties["QTD"] = {"number": data["QTD"]}
    if "YTD" in data:
        properties["YTD"] = {"number": data["YTD"]}
    
    # 更新时间
    properties["更新时间"] = {
        "date": {"start": datetime.now().strftime("%Y-%m-%d")}
    }
    
    payload = {"properties": properties}
    
    response = requests.patch(url, headers=headers, json=payload)
    return response.status_code == 200

def update_notion_database(market_data: dict):
    """更新整个Notion数据库"""
    pages = get_notion_pages()
    
    for page in pages:
        # 获取资产名称
        title_prop = page["properties"].get("资产名称", {})
        if title_prop.get("title") and len(title_prop["title"]) > 0:
            name = title_prop["title"][0]["plain_text"]
            
            if name in market_data:
                success = update_notion_page(page["id"], market_data[name])
                status = "✓" if success else "✗"
                print(f"{status} 更新 {name}")
                time.sleep(0.3)  # 避免API限流

# ============== 主程序 ==============

def main():
    print("=" * 50)
    print("市场数据矩阵更新器")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    if not NOTION_API_KEY:
        print("错误: 请设置 NOTION_API_KEY 环境变量")
        print("1. 访问 https://www.notion.so/my-integrations 创建集成")
        print("2. 复制 API Key")
        print("3. 创建 .env 文件: NOTION_API_KEY=your_key_here")
        return
    
    # 获取市场数据
    print("\n[1/2] 获取市场数据...")
    market_data = fetch_all_data()
    print(f"成功获取 {len(market_data)} 个资产数据")
    
    # 更新Notion
    print("\n[2/2] 更新Notion数据库...")
    update_notion_database(market_data)
    
    print("\n" + "=" * 50)
    print("更新完成!")
    print("=" * 50)

if __name__ == "__main__":
    main()
