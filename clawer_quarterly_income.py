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
    """從 stocks 資料表抓全部股票 id + stock_no"""
    conn = pyodbc.connect(CONN_STR)
    df = pd.read_sql("SELECT id, stock_no FROM dbo.stocks ORDER BY id", conn)
    conn.close()
    return df

def clawer_quarterly_income(stock_no):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    driver = webdriver.Chrome(service=Service(), options=chrome_options)

    # 損益表格
    driver.get(f"https://www.cmoney.tw/forum/stock/{stock_no}?s=income-statement")
    time.sleep(5)

    # 損益表按鈕
    income_button = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[3]/div[3]/div')
    income_button.click()
    time.sleep(3)

    # 損益表格
    income_table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[3]/div[2]')
    income_df = pd.read_html(income_table.get_attribute('outerHTML'))[0]
    income_df = flatten_columns(income_df)

    # EPS表格
    driver.get(f"https://www.cmoney.tw/forum/stock/{stock_no}?s=eps")
    time.sleep(5)

    # EPS表格
    eps_table = driver.find_element(By.XPATH, '//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[3]')
    eps_df = pd.read_html(eps_table.get_attribute('outerHTML'))[0]
    eps_df = flatten_columns(eps_df)

    return income_df, eps_df

def flatten_columns(df):
    # 若 columns 是 MultiIndex → 轉成單層
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(x) for x in col]).strip() for col in df.columns]
    return df

def build_quarterly_income_df(df_income: pd.DataFrame, df_eps: pd.DataFrame) -> pd.DataFrame:
    # --- 損益表處理 ---
    inc = df_income.copy()
    # 確保欄位名稱正確
    inc = inc.rename(columns={
        "日期": "period",
        "營收": "revenue",
        "毛利": "gross_profit",
        "營業利益": "operating_income",
        "稅後淨利": "net_income"
    })

    # "2025/Q3" -> 年、季
    inc["period"] = inc["period"].astype(str)
    inc["fiscal_year"] = inc["period"].str.slice(0, 4).astype(int)
    # 從第 6 個字元開始 "Q3" -> 取 3
    inc["fiscal_quarter"] = inc["period"].str.slice(5).str.replace("Q", "", regex=False).astype(int)
    inc["roc_year"] = inc["fiscal_year"] - 1911

    # 數字欄位轉成 float
    num_cols = [
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income"
    ]
    for col in num_cols:
        inc[col] = pd.to_numeric(inc[col], errors="coerce")

    # --- EPS 表處理 ---
    eps = df_eps.copy()
    eps = eps.rename(columns={
        "年度/季別": "period",
        "每股盈餘": "eps_basic"
    })

    eps["period"] = eps["period"].astype(str)

    # 過濾掉「2025合計」「2024合計」這種年度合計，只留 "2025/Q3" 這種
    eps = eps[eps["period"].str.contains("/")].copy()

    eps["fiscal_year"] = eps["period"].str.slice(0, 4).astype(int)
    eps["fiscal_quarter"] = eps["period"].str.slice(5).str.replace("Q", "", regex=False).astype(int)

    # EPS 轉 float
    eps["eps_basic"] = pd.to_numeric(eps["eps_basic"], errors="coerce")

    # --- 合併損益表 + EPS ---
    df = pd.merge(
        inc,
        eps[["fiscal_year", "fiscal_quarter", "eps_basic"]],
        on=["fiscal_year", "fiscal_quarter"],
        how="left"
    )

    df = df[[
        "fiscal_year",
        "fiscal_quarter",
        "roc_year",
        "revenue",
        "gross_profit",
        "operating_income",
        "net_income",
        "eps_basic"
    ]]

    return df

def insert_quarterly_income_to_db(stock_id: int, df: pd.DataFrame):
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    sql = """
    INSERT INTO dbo.stock_quarterly_income (
        stock_id,
        fiscal_year,
        fiscal_quarter,
        roc_year,
        revenue,
        gross_profit,
        operating_income,
        net_income,
        eps_basic
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            to_float_scaled(row.get("revenue")),
            to_float_scaled(row.get("gross_profit")),
            to_float_scaled(row.get("operating_income")),
            to_float_scaled(row.get("net_income")),
            row["eps_basic"]
        )

        try:
            cursor.execute(sql, params)
        except pyodbc.IntegrityError as ex:
                print(f"  ⚠️ 略過重複 {stock_id} {row['fiscal_year']}Q{row['fiscal_quarter']}: {ex}")
        except Exception as ex:
            print(f"  ❌ 寫入失敗 {stock_id} {row['fiscal_year']}Q{row['fiscal_quarter']}: {ex}")

    conn.commit()
    cursor.close()
    conn.close()

def process_quarterly_income_for_stock(stock_id, stock_no):
    try:
        income_table, eps_table = clawer_quarterly_income(stock_no)
        qi_df = build_quarterly_income_df(income_table, eps_table)
        insert_quarterly_income_to_db(stock_id, qi_df)
        print(f"✅ 已將 {stock_no} 寫入 stock_quarterly_income，共 {len(qi_df)} 筆")
    except Exception as ex:
        print(f"❌ {stock_no} 失敗：{ex}")