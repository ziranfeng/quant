# This script extract stock price for given period & interval
# and write to cloud storage

from datetime import datetime as dt
import pymysql
import yfinance as yf

from yitian.datasource import *
from yitian.datasource import file_utils, preprocess, PRIVATE_HOST, USER, DATABASE, EQUITY

# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | ticker        | ['MSFT']         | the target year for data extraction      |
# | period        | '1d'             | data extraction period                   |
# | table_name    | 'nasdaq_daily'   | the output table in DB                   |
# | connection    | connection       | the output table in DB                   |
connection = locals()['connection']
ticker = locals()['ticker']
period = locals()['period']
table_name = locals()['table_name']


# Set up cloud sql connections
password = 'jinyongwuxia'
connection = pymysql.connect(host=PRIVATE_HOST,
                             user=USER,
                             password=password,
                             db=DATABASE)

# ====================================================================================================================

period = '2y' if period.isin(['5y', '10y', 'max']) else period

# Read In Data

with connection.cursor() as cursor:
    for index, row in ts_pd.iterrows():
        sql = """
        INSERT INTO {table_name}({ticker}, {datetime}, {open}, {high}, {low}, {close}, {volume}, {year}, {month}, {day}, {updated_at}) 
        VALUES('{ticker_v}', '{date_v}', {open_v}, {high_v}, {low_v}, {close_v}, {volume_v}, {year_v}, {month_v}, {day_v}, '{updated_at_v}')
        """.format(table_name=table_name,
                   ticker=TICKER,
                   datetime=DATETIME,
                   open=OPEN, high=HIGH, low=LOW, close=CLOSE, volume=VOLUME,
                   year=YEAR, month=MONTH, day=DAY, updated_at=UPDATED_AT,
                   ticker_v=row[TICKER],
                   date_v=row[DATETIME],
                   open_v=row[OPEN], high_v=row[HIGH], low_v=row[LOW], close_v=row[CLOSE], volume_v=row[VOLUME],
                   year_v=row[YEAR], month_v=row[MONTH], day_v=row[DAY], updated_at_v=row[UPDATED_AT])

        print(sql)
        cursor.execute(sql)

        # connection is not autocommit by default. So you must commit to save your changes.
        connection.commit()

cursor.close()
