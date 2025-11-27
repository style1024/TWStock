import time
import selenium
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import pandas as pd
import pyodbc
from datetime import datetime

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;" 
    "DATABASE=Stock;"
    "Trusted_Connection=yes;"
)

def get_stocks():
    conn = pyodbc.connect(CONN_STR)
    df = pd.read_sql("SELECT id, stock_no FROM dbo.stocks ORDER BY id", conn)
    conn.close()
    return df


def clawer_dividend(stock_no):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(), options=chrome_options)


    driver.get(f"https://www.cmoney.tw/forum/stock/{stock_no}?s=dividend")
    time.sleep(5)
    # 點選按鈕
    button = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/section[2]/div[4]/div')
    button.click()

    table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/section[2]/div[3]')

    html = table.get_attribute('outerHTML')

    dfs = pd.read_html(html)
    df = dfs[0]

    return df

# 把 MultiIndex 轉成單層欄位
def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(x) for x in col]).strip() for col in df.columns]
    return df

def transform_dividend_df(df):
    df_clean = pd.DataFrame()

    df_clean["cash_dividend"] = pd.to_numeric(df["現金股利(元)_股利"], errors="coerce")

    # 日期格式清洗（2025/03/17 → 2025-03-17）
    def clean_date(x):
        try:
            return datetime.strptime(str(x), "%Y/%m/%d").date()
        except:
            return None

    df_clean["ex_dividend_date"] = df["現金股利(元)_除息日"].apply(clean_date)
    df_clean["pay_date"] = df["現金股利(元)_發放日"].apply(clean_date)

    # 年度（若你未來要放，可留著）
    df_clean["fiscal_year"] = df["除權息年度_除權息年度"].astype(int)

    return df_clean

def insert_dividend_to_db(stock_no, df):
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    sql = """
        INSERT INTO dbo.stock_dividend (
            stock_no,
            cash_dividend,
            ex_dividend_date,
            pay_date
        )
        VALUES (?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        params = (
            stock_no,
            row["cash_dividend"],
            row["ex_dividend_date"],
            row["pay_date"]
        )
        try:
            cursor.execute(sql, params)
        except Exception as ex:
            print(f"❌ 寫入錯誤：{stock_no} {row} | {ex}")

    conn.commit()
    cursor.close()
    conn.close()

def process_dividend_for_stock(stock_no):
    try:
        df_cd = clawer_dividend(stock_no)
        df_cd = flatten_columns(df_cd)
        # 過濾年度
        df_cd = df_cd[df_cd["除權息年度_除權息年度"] >= 2020]
        # 格式清洗
        df_clean = transform_dividend_df(df_cd)
        # 寫入 DB
        insert_dividend_to_db(stock_no, df_clean)
        print(f"✅ 已寫入 {stock_no} 的股利資料（2020~最新）")
    except Exception as ex:
        print(f"❌ {stock_no} 股利資料失敗：{ex}")