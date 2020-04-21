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

data_pd = yf.download(  # or pdr.get_data_yahoo(...
    # tickers list or string as well
    tickers = ticker,

    # use "period" instead of start/end
    # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    # (optional, default is '1mo')
    period = period,

    # fetch data by interval (including intraday if period < 60 days)
    # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    # (optional, default is '1d')
    interval = '60m',

    # group by ticker (to access via data['SPY'])
    # (optional, default is 'column')
    group_by = 'ticker',

    # adjust all OHLC automatically
    # (optional, default is False)
    auto_adjust = True,

    # download pre/post regular market hours data
    # (optional, default is False)
    prepost = True,

    # use threads for mass downloading? (True/False/Integer)
    # (optional, default is True)
    threads = True,

    # proxy URL scheme use use when downloading?
    # (optional, default is None)
    proxy = None
)


# Standardize data_pd

data_pd = data_pd.reset_index().rename(columns={'Datetime': DATETIME,
                                                'Open': OPEN,
                                                'High': HIGH,
                                                'Low': LOW,
                                                'Close': CLOSE,
                                                'Volume': VOLUME})

data_pd[DATETIME] = [t[:19] for t in data_pd[DATETIME].astype(str)]
ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True, index_col=DATETIME)
ts_pd = preprocess.add_ymd(ts_pd, index_col=DATETIME)
ts_pd[TICKER] = ticker
ts_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")


# Write historical price to data warehouse by year
for year, grouped_pd in ts_pd.groupby(YEAR):
    bucket_path = file_utils.create_data_path(EQUITY, 'company', ticker.lower(), str(year), 'hourly.csv')
    grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')
    print(f"{ticker} in year {year} has been write to {bucket_path}")


# Insert data into SQL table
ts_pd.reset_index(inplace=True)

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
