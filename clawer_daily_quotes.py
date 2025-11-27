import requests
import pandas as pd
import pyodbc
from datetime import date

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=.\\MSSQLSERVER_2021;"
    "DATABASE=Stock;"
    "Trusted_Connection=yes;"
)

def roc_str_to_date(roc_str: str) -> date:
    roc_str = str(roc_str).strip()
    y, m, d = roc_str.split("/")
    year = int(y) + 1911
    month = int(m)
    day = int(d)
    return date(year, month, day)

def transform_twse_stock_day_json(json_data: dict) -> pd.DataFrame:
    # 原始表格
    raw = pd.DataFrame(json_data["data"], columns=json_data["fields"])

    # 改欄位名稱方便使用
    raw = raw.rename(columns={
        "日期": "date_roc",
        "成交股數": "volume_shares",
        "成交金額": "turnover",
        "開盤價": "open_price",
        "最高價": "high_price",
        "最低價": "low_price",
        "收盤價": "last_price",
        "漲跌價差": "change_price",
        "成交筆數": "transactions",
    })

    # 日期：民國 → 西元
    raw["trade_date"] = raw["date_roc"].apply(roc_str_to_date)

    # 小工具：數字轉換
    def to_int(s):
        s = str(s).replace(",", "").strip()
        if s == "" or s == "0":
            return 0
        return int(float(s))

    def to_float(s):
        s = str(s).replace(",", "").strip()
        if s in ("", "X0.00", "--"):
            return 0.0
        s = s.replace("X", "")
        return float(s)

    # 轉型各欄位
    raw["volume_shares"] = raw["volume_shares"].apply(to_int)
    raw["open_price"]    = raw["open_price"].apply(to_float)
    raw["high_price"]    = raw["high_price"].apply(to_float)
    raw["low_price"]     = raw["low_price"].apply(to_float)
    raw["last_price"]    = raw["last_price"].apply(to_float)
    raw["change_price"]  = raw["change_price"].apply(to_float)

    # 前一日收盤價 = 當日收盤價 - 漲跌價差
    def calc_prev_close(row):
        lp = row["last_price"]
        cp = row["change_price"]
        if lp is None or cp is None:
            return None
        return lp - cp

    raw["prev_close"] = raw.apply(calc_prev_close, axis=1)

    # 成交量（張）
    raw["volume_lots"] = (raw["volume_shares"] // 1000).astype(int)

    # 只保留要塞 DB 的欄位
    df = raw[[
        "trade_date",
        "last_price",
        "open_price",
        "high_price",
        "low_price",
        "prev_close",
        "volume_lots",
        "volume_shares",
    ]].copy()

    return df

def insert_daily_quotes_to_db(stock_id: int, df: pd.DataFrame):
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()

    sql = """
    INSERT INTO dbo.stock_daily_quotes (
        stock_id,
        trade_date,
        last_price,
        open_price,
        high_price,
        low_price,
        prev_close,
        volume_lots,
        volume_shares
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    for _, row in df.iterrows():
        params = (
            stock_id,
            row["trade_date"],
            row["last_price"],
            row["open_price"],
            row["high_price"],
            row["low_price"],
            row["prev_close"],
            int(row["volume_lots"]),
            int(row["volume_shares"]),
        )
        try:
            cursor.execute(sql, params)
        except pyodbc.IntegrityError as ex:
            # 建議在 DB 上有 UNIQUE(stock_id, trade_date) 時，重複就會走這裡
            print(f"⚠️ 重複略過 {stock_id} {row['trade_date']}: {ex}")
        except Exception as ex:
            print(f"❌ 寫入失敗 {stock_id} {row['trade_date']}: {ex}")

    conn.commit()
    cursor.close()
    conn.close()

def fetch_twse_stock_day_json(stock_no: str, yyyymm: str) -> dict:
    url = (
        "https://www.twse.com.tw/exchangeReport/STOCK_DAY"
        f"?date={yyyymm}&stockNo={stock_no}"
    )
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.json()


def fetch_and_save_stock_month(stock_id: int, stock_no: str, yyyymm: str):
    json_data = fetch_twse_stock_day_json(stock_no, yyyymm)

    if json_data.get("stat") != "OK":
        print(f"❌ TWSE 回傳失敗，股票代號{stock_no}：{json_data.get('stat')}")
        return

    df = transform_twse_stock_day_json(json_data)
    insert_daily_quotes_to_db(stock_id, df)
    print(f"✅ 已寫入 {stock_no}（stock_id={stock_id}） {yyyymm} 共 {len(df)} 筆日行情")

def get_stocks():
    conn = pyodbc.connect(CONN_STR)
    df = pd.read_sql("SELECT id, stock_no FROM dbo.stocks ORDER BY id", conn)
    conn.close()
    return df

if __name__ == "__main__":
    stocks = get_stocks()
    yyyymm_list = ["202511", "20251031", "20250930"]  # 最近三個月

    for _, row in stocks.iterrows():
        stock_id = row["id"]
        stock_no = row["stock_no"]
        for yyyymm in yyyymm_list:
            fetch_and_save_stock_month(stock_id, stock_no, yyyymm)