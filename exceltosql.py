import pandas as pd
import pyodbc
import re

# ======= 設定區 =======

CSV_PATH = r"StockList.csv"  # TODO: 改成你實際的 csv 路徑

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\MSSQLSERVER_2021;"          # TODO: 改成你的 SQL Server 伺服器，例如 localhost\SQLEXPRESS
    "DATABASE=Stock;"            # TODO: 改成你的資料庫名稱
    "Trusted_Connection=yes;"    # 如果是 SQL 帳號登入就改成 UID/PWD
)

# ======= 工具函式 =======

def clean_stock_no(x):
    """把 =\"2330\" 這種 Excel 字串清掉，但保留字母，例如 2882A"""
    if pd.isna(x):
        return None
    s = str(x).strip()

    # case 1: ="2330" → 2330
    if s.startswith('="') and s.endswith('"'):
        return s[2:-1]

    # case 2: 一般情況 → 原樣保留
    return s

def map_market(x):
    """Excel 市場欄位轉成 stocks.market"""
    s = str(x).strip()
    if s == "市":
        return "TSE"
    if s == "櫃":
        return "OTC"
    return s  # 其他情況先原樣放

def to_float_safe(x):
    """把各種奇怪的市值字串轉成 float，錯的就回 None"""
    if pd.isna(x):
        return None
    s = str(x).replace(",", "").strip()
    if s == "":
        return None
    try:
        return float(s)
    except ValueError:
        return None

# ======= 主流程 =======

def import_csv_to_stocks():
    # 1. 讀 CSV
    df = pd.read_csv(CSV_PATH)

    # 2. 選需要的欄位（欄位名稱要跟你的 CSV 檔頭一樣）
    df = df[["代號", "名稱", "市場", "市值(億)", "產業別"]].copy()

    # 3. 清洗 & Mapping
    df["stock_no"] = df["代號"].apply(clean_stock_no)
    df["name"] = df["名稱"].astype(str)
    df["market"] = df["市場"].apply(map_market)
    df["market_cap"] = df["市值(億)"].apply(to_float_safe)
    df["industry"] = df["產業別"].astype(str)

    # 只留真正要寫入的欄位，且股票代碼不能是空的
    df_db = df[["stock_no", "name", "market", "market_cap", "industry"]]
    df_db = df_db[df_db["stock_no"].notna()]

    # 4. 連線 SQL Server
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    # optional：加速大量 insert
    cursor.fast_executemany = True

    insert_sql = """
    INSERT INTO dbo.stocks (stock_no, name, market, market_cap, industry, is_active)
    VALUES (?, ?, ?, ?, ?, 1);
    """

    # 5. 一筆一筆插入（全部都是新增）
    rows = df_db.to_records(index=False)

    for row in rows:
        stock_no, name, market, market_cap, industry = row
        cursor.execute(
            insert_sql,
            stock_no,
            name,
            market,
            market_cap,
            industry,
        )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"匯入完成，共寫入 {len(df_db)} 筆資料。")


if __name__ == "__main__":
    import_csv_to_stocks()
