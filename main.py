import sys
import requests
import json
import pandas as pd
from bs4 import BeautifulSoup
from FinMind.data import DataLoader
from exceltosql import excel_to_sql
import datetime

# 讀取 appsettings.json
with open('appsettings.json', 'r', encoding='utf-8') as f:
    config = json.load(f)

finmind_token = config.get('FINMIND_TOKEN', None)

def get_yesterday():
    today = datetime.date.today()
    delta = datetime.timedelta(days=1)
    return today - delta

def get_basic_info(stock_id):
    api = DataLoader()
    if finmind_token:
        api.login_by_token(api_token=finmind_token)
    else:
        raise Exception("未設定 FINMIND_TOKEN，請在 appsettings.json 加入您的token。")
    
    df = api.taiwan_stock_info()
    info = df[df['stock_id'] == stock_id]
    if info.empty:
        return {}
    row = info.iloc[0]
    result = {
        '公司名稱': row['stock_name'],
        '股票代碼': row['stock_id'],
        '產業別': row['industry_category'],
    }

    # 股價、市值
    price_df = api.taiwan_stock_daily(stock_id=stock_id, start_date=get_yesterday().isoformat(), end_date=get_yesterday().isoformat())
    if not price_df.empty:
        price = price_df.iloc[-1]
        result["當前股價"] = price.get("close")
    else:
        result["當前股價"] = None
        result["市值(百萬)"] = None
    
    # 基本面指標
    # profile = api.taiwan_stock_financial_statement(stock_id=stock_id)
    # 修正：篩掉 date 為 NaT
    profile = profile.dropna(subset=["date"])
    if not profile.empty:
        last = profile.sort_values('date').iloc[-1]
        # 若這些欄位存在才取值
        result["本益比(PER)"] = last.get("EPS") and price.get("close", 0)/last["EPS"] if last.get("EPS") else None
        result["股價淨值比(PBR)"] = last.get("BPS") and price.get("close", 0)/last["BPS"] if last.get("BPS") else None
        result["每股盈餘(EPS)"] = last.get("EPS")
        result["ROE(%)"] = last.get("ROE")
        result["發行股數"] = row['stock_total']
    else:
        result["本益比(PER)"] = None
        result["股價淨值比(PBR)"] = None
        result["每股盈餘(EPS)"] = None
        result["ROE(%)"] = None
        result["發行股數"] = row['stock_total']

    # 你可在這裡加 Goodinfo 殖利率/營收爬蟲
    
    return result

def get_goodinfo_profile(stock_id):
    # Goodinfo網頁爬蟲補充殖利率、配息、營收成長率
    url = f'https://goodinfo.tw/tw/StockDividendPolicy.asp?STOCK_ID={stock_id}'
    headers = {'User-Agent': 'Mozilla/5.0'}
    soup = BeautifulSoup(requests.get(url, headers=headers).text, 'html.parser')
    result = {}
    # Example: 配息表、殖利率，營收MoM/YoY見「財報分析」分頁
    tables = soup.select('table.b1.p4_2.r0_10.row_bg_2n')
    if tables:
        # 配息表
        trs = tables[0].find_all('tr')
        result['殖利率(%)'] = trs[2].find_all('td')[3].text.strip() if len(trs) > 2 else None
        result['配息'] = trs[2].find_all('td')[4].text.strip() if len(trs) > 2 else None
    # You can add 營收 YoY/MoM by parsing 財報頁
    return result

def get_chip_info(stock_id):
    api = DataLoader()
    # 法人買超籌碼
    chip = api.taiwan_stock_institutional_investors(stock_id=stock_id, start_date=get_yesterday().isoformat(), end_date=get_yesterday().isoformat())
    result = {}
    if not chip.empty:
        res = chip.iloc[-1]
        result = {
            '外資買賣超': res.get('Foreign_Investor'),
            '投信買賣超': res.get('Investment_Trust'),
            '自營商買賣超': res.get('Dealer_Total'),
        }
    # 股東結構與大戶
    url = f'https://goodinfo.tw/tw/StockHolderStat.asp?STOCK_ID={stock_id}'
    soup = BeautifulSoup(requests.get(url, headers={'user-agent':'Mozilla/5.0'}).text, 'html.parser')
    # >400, >1000持股比率和總股東人數
    holders = {}
    tables = soup.select('table.b1.p4_2.r0_10.row_bg_2n')
    if tables:
        rows = tables[0].find_all('tr')
        for row in rows:
            tds = row.find_all('td')
            if len(tds) > 5 and '張' in tds[0].text:  # 例如"持股400~899張"
                if '400' in tds[0].text: holders['>400張持股比率'] = tds[2].text.strip()
                if '1000' in tds[0].text: holders['>1000張持股比率'] = tds[2].text.strip()
            if '總股東人數' in tds[0].text:
                holders['總股東人數'] = tds[2].text.strip()
    result.update(holders)
    # 大戶/散戶週月變化 => 自己計算或用 CMoney API
    # 通常 Goodinfo 股東結構有近月週日資料
    return result

def get_tech_info(stock_id, days=365):
    api = DataLoader()
    start_date = (datetime.date.today() - datetime.timedelta(days=days)).isoformat()
    end_date = get_yesterday().isoformat()
    # 回傳 K 線日資料
    data = api.taiwan_stock_daily(stock_id=stock_id, start_date=start_date, end_date=end_date)
    if data.empty:
        return []
    # [{日期, open, high, low, close, volume}, ...]
    return data[['date', 'open', 'high', 'low', 'close', 'volume']].to_dict(orient='records')

def main():
    stock_id = input("請輸入台股股票代號：").strip()
    print("\n基本面：")
    basic = get_basic_info(stock_id)
    for k, v in basic.items(): print(f"{k}: {v}")
    print("\n籌碼面：")
    chip = get_chip_info(stock_id)
    for k, v in chip.items(): print(f"{k}: {v}")
    print("\n技術面：K線資料，最近一年")
    tech = get_tech_info(stock_id)
    df = pd.DataFrame(tech)
    print(df.tail(10))  # 只顯示最近10日
  
if __name__ == "__main__":
    excel_to_sql()