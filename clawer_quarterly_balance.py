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
    "SERVER=.\MSSQLSERVER_2021;"      # 改成你的 SQL Server
    "DATABASE=Stock;"
    "Trusted_Connection=yes;"
)

def get_stocks():
    """從 stocks 資料表抓全部股票 id + stock_no"""
    conn = pyodbc.connect(CONN_STR)
    df = pd.read_sql("SELECT id, stock_no FROM dbo.stocks ORDER BY id", conn)
    conn.close()
    return df

def clawer_quarterly_balance(stock_no):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    driver.get(f"https://www.cmoney.tw/forum/stock/{stock_no}?s=balance-sheet")
    time.sleep(5)

    # 點選按鈕
    assets_button = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[1]/div[1]/div[1]/label[1]')
    liabilities_button = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[1]/div[1]/div[1]/label[2]')
    equity_button = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[1]/div[1]/div[1]/label[3]')
    
    # 資產表
    assets_button.click()
    time.sleep(3)
    assets_table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[3]/div[2]')
    assets_html = assets_table.get_attribute('outerHTML')
    assets_dfs = pd.read_html(assets_html)
    assets_df = assets_dfs[0]

    # 負債表
    liabilities_button.click()
    time.sleep(3)
    liabilities_table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[3]/div[2]')
    liabilities_html = liabilities_table.get_attribute('outerHTML')
    liabilities_dfs = pd.read_html(liabilities_html)
    liabilities_df = liabilities_dfs[0]

    # 權益表
    equity_button.click()
    time.sleep(3)
    equity_table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[3]/div[2]')
    equity_html = equity_table.get_attribute('outerHTML')
    equity_dfs = pd.read_html(equity_html)
    equity_df = equity_dfs[0]

    return assets_df, liabilities_df, equity_df

def flatten_columns(df):
    # 若 columns 是 MultiIndex → 轉成單層
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(x) for x in col]).strip() for col in df.columns]
    return df

# 把三張表合併成「季資產負債」DataFrame
def build_quarterly_balance_df(assets_df, liabilities_df, equity_df) -> pd.DataFrame:
    # 扁平欄位
    assets_df = flatten_columns(assets_df)
    liabilities_df = flatten_columns(liabilities_df)
    equity_df = flatten_columns(equity_df)

    # 只留需要的欄位
    a = assets_df[["日期", "總資產"]].copy()

    # 負債表：日期, ..., 總負債
    l = liabilities_df[["日期", "總負債"]].copy()

    # 權益表：日期, 股本, 股東權益(淨值), 季收盤價
    e = equity_df[["日期", "股東權益(淨值)"]].copy()

    # 依日期 merge
    df = a.merge(l, on="日期").merge(e, on="日期")

    # 拆出年度與季別
    df["fiscal_year"] = df["日期"].str.slice(0, 4).astype(int)  # 2025
    # "2025/Q3" → "3"
    df["fiscal_quarter"] = (
        df["日期"].str.slice(5).str.replace("Q", "", regex=False).astype(int)
    )
    df["roc_year"] = df["fiscal_year"] - 1911

    # 數字欄位轉成 float（目前單位：仟元）
    for col_old, col_new in [
        ("總資產", "total_assets"),
        ("總負債", "total_liabilities"),
        ("股東權益(淨值)", "total_equity"),
    ]:
        df[col_new] = (
            df[col_old]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace("-", "0", regex=False)
        )
        df[col_new] = pd.to_numeric(df[col_new], errors="coerce")

    df = df[df["fiscal_year"] >= 2022]

    return df[
        [
            "fiscal_year",
            "fiscal_quarter",
            "roc_year",
            "total_assets",
            "total_equity",
            "total_liabilities",
        ]
    ]

# 寫入 stock_quarterly_balance
def insert_quarterly_balance_to_db(stock_id: int, df: pd.DataFrame):
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    sql = """
    INSERT INTO dbo.stock_quarterly_balance (
        stock_id,
        fiscal_year,
        fiscal_quarter,
        roc_year,
        total_assets,
        total_equity,
        total_liabilities
    )
    VALUES (?, ?, ?, ?, ?, ?, ?)
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
            return float(val) * 1000.0 / 10_000_000.0
        except Exception:
            return None

    for _, row in df.iterrows():
        params = (
            stock_id,
            int(row["fiscal_year"]),
            int(row["fiscal_quarter"]),
            int(row["roc_year"]),
            to_float_scaled(row.get("total_assets")),
            to_float_scaled(row.get("total_equity")),
            to_float_scaled(row.get("total_liabilities")),
        )
        try:
            cursor.execute(sql, params)
        except pyodbc.IntegrityError as ex:
            print(f"  ⚠️ 重複略過 {stock_id} {row['fiscal_year']}Q{row['fiscal_quarter']}: {ex}")
        except Exception as ex:
            print(f"  ❌ 寫入失敗 {stock_id} {row['fiscal_year']}Q{row['fiscal_quarter']}: {ex}")

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    stocks = get_stocks()

    for _, row in stocks.iterrows():
        stock_id = row["id"]
        stock_no = row["stock_no"]

        print(f"====== 處理 {stock_no} (id={stock_id}) ======")

        try:
            assets_df, liabilities_df, equity_df = clawer_quarterly_balance(stock_no)
            qb_df = build_quarterly_balance_df(assets_df, liabilities_df, equity_df)
            insert_quarterly_balance_to_db(stock_id, qb_df)
            print(f"✅ 已將 {stock_no} 寫入 stock_quarterly_balance，共 {len(qb_df)} 筆")
        except Exception as ex:
            print(f"❌ {stock_no} 失敗：{ex}")

        # 稍微睡一下，避免被網站擋
        time.sleep(2)