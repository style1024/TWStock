from lxml import html
import requests
import pyodbc
from datetime import datetime

CONN_STR = (
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost;" 
    "DATABASE=Stock;"
    "Trusted_Connection=yes;"
)

def fetch_daily_quote(stock_no):
    url = f"https://www.cmoney.tw/forum/stock/{stock_no}"
    response = requests.get(url)
    byte_data = response.content
    source_code = html.fromstring(byte_data)

    prev_close = source_code.xpath('//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[2]/div[1]/div[1]/div[6]/span[2]')[0]
    open_price = source_code.xpath('//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[2]/div[1]/div[1]/div[2]/span[2]')[0]
    high_price = source_code.xpath('//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[2]/div[1]/div[1]/div[3]/span[2]')[0]
    low_price = source_code.xpath('//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[2]/div[1]/div[1]/div[4]/span[2]')[0]
    last_price = source_code.xpath('//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[2]/div[1]/div[1]/div[1]/span[2]')[0]
    volume_lots = source_code.xpath('//*[@id="StockRevPanel"]/div[3]/div[2]/section/div/div[2]/div[2]/div[1]/div[2]/div[7]/span[2]')[0]

    # 寫入資料庫
    conn = pyodbc.connect(CONN_STR)
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO dbo.stock_daily_quotes (
        stock_no,
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
    """, (
        stock_no,
        datetime.now().date(),
        last_price.text_content().strip().replace(",", ""),
        open_price.text_content().strip().replace(",", ""),
        high_price.text_content().strip().replace(",", ""),
        low_price.text_content().strip().replace(",", ""),
        prev_close.text_content().strip().replace(",", ""),
        volume_lots.text_content().strip().replace(",", ""),
        int(volume_lots.text_content().strip().replace(",", "")) * 1000
    ))

    print(f"Inserted daily quote for stock {stock_no}")

    conn.commit()
    cursor.close()
    conn.close()


if __name__ == "__main__":
    stock_no = "2330"  # Example stock number
    fetch_daily_quote(stock_no)