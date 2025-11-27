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

# 爬取月營收資料
def clawer_monthly_revenue(stock_no):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(), options=chrome_options)


    driver.get(f"https://www.cmoney.tw/forum/stock/{stock_no}?s=revenue")
    time.sleep(5)
    # 點選按鈕
    button = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[3]/div')
    button.click()

    table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]')

    html = table.get_attribute('outerHTML')

    dfs = pd.read_html(html)
    df = dfs[0]

    return df

# 重新整理並清洗月營收資料
def transform_monthly_df(df: pd.DataFrame) -> pd.DataFrame:
    # 重新命名欄位成好用一點的英文
    df = df.rename(columns={
        "年度/月份_年度/月份": "ym",
        "營業收入_當月營收": "revenue_current",
        "營業收入_去年同月營收": "revenue_prev_year_month",
        "累積營業收入_當月累計營收": "revenue_ytd",
        "累積營業收入_去年累計營收": "revenue_ytd_prev_year",
    })

    # 確保 ym 是字串，才能用 .str
    df["ym"] = df["ym"].astype(str)

    # 拆成年、月
    df["year"] = df["ym"].str.slice(0, 4).astype(int)
    df["month"] = df["ym"].str.slice(5, 7).astype(int)
    df["roc_year"] = df["year"] - 1911

    df = df[
        ((df["year"] > 2022) | ((df["year"] == 2022) & (df["month"] >= 9))) &
        ((df["year"] < 2025) | ((df["year"] == 2025) & (df["month"] <= 9)))
    ]

    # 轉數字欄位
    numeric_cols = [
        "revenue_current",
        "revenue_prev_year_month",
        "revenue_ytd",
        "revenue_ytd_prev_year",
    ]

    for col in numeric_cols:
        df[col] = (
            df[col]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("%", "", regex=False)
        )
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df

# 把 MultiIndex 轉成單層欄位
def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(x) for x in col]).strip() for col in df.columns]
    return df

# 將清洗好的月營收資料寫入資料庫
def insert_monthly_to_db(stock_id: int, df: pd.DataFrame):
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    sql = """
    INSERT INTO dbo.stock_monthly_revenue (
        stock_id,
        year,
        month,
        roc_year,
        revenue_current,
        revenue_prev_year_month,
        revenue_ytd,
        revenue_ytd_prev_year
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """

    def to_float_scaled(val):
        # 將數值轉為 float，NaN 或無效值回傳 None 以便寫入 SQL 的 NULL
        try:
            if pd.isna(val):
                return None
            # 有些情況可能是字串空白或空字串
            if isinstance(val, str):
                v = val.strip()
                if v == "":
                    return None
                val = float(v)
            return float(val) * 1000.0 / 100_000_000.0
        except Exception:
            return None

    for _, row in df.iterrows():
        params = (
            int(stock_id),
            int(row["year"]),
            int(row["month"]),
            int(row["roc_year"]),
            to_float_scaled(row.get("revenue_current")),
            to_float_scaled(row.get("revenue_prev_year_month")),
            to_float_scaled(row.get("revenue_ytd")),
            to_float_scaled(row.get("revenue_ytd_prev_year"))
        )
        cursor.execute(sql, params)

    conn.commit()
    cursor.close()
    conn.close()

def process_monthly_revenue_for_stock(stock_id, stock_no):
    try:
        df_cmr = clawer_monthly_revenue(stock_no)
        df_cmr = flatten_columns(df_cmr)
        df_clean  = transform_monthly_df(df_cmr)
        insert_monthly_to_db(stock_id=stock_id, df=df_clean)
        print(f"✅ 已將{stock_no}寫入 stock_monthly_revenue")
    except Exception as ex:
        print(f"❌ {stock_no} 月營收資料失敗：{ex}")