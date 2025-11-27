import pandas as pd
import pyodbc
import exceltosql
import clawer_dividend as cd
import clawer_monthly_revenue as cmr
import clawer_daily_quotes as cdq
import clawer_quarterly_balance as cqb
import clawer_quarterly_income as cqi

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

def flatten_columns(df):
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ['_'.join([str(x) for x in col]).strip() for col in df.columns]
    return df

# 檢查stocks是否有資料表，如果有資料救回傳true
def check_stocks_table():
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*) 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = 'stocks'
    """)
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] > 0

if __name__ == "__main__":
    # 1. 匯入股票清單到 stocks 資料表
    if not check_stocks_table():
        exceltosql.import_csv_to_stocks()

    # 2. 取得 stocks 資料表的股票清單
    df_stocks = get_stocks()
    for index, row in df_stocks.iterrows():
        stock_no = row["stock_no"]
        stock_id = row["id"]
        # 爬取每支股票的股利資料
        cd.process_dividend_for_stock(stock_no)
        # 爬取每支股票的月營收資料
        cmr.process_monthly_revenue_for_stock(stock_id, stock_no)
        # 爬取每支股票的日成交資料
        cdq.process_daily_quotes_for_stock(stock_id, stock_no)
        # 爬取每支股票的季報資料（資產負債表 + 綜合損益表）
        cqb.process_quarterly_balance_for_stock(stock_id, stock_no)
        # 爬取每支股票的季報資料（綜合損益表 + EPS
        cqi.process_quarterly_income_for_stock(stock_id, stock_no)
