from datetime import datetime as dt
import pandas as pd
import pymysql
import yfinance as yf

from yitian.datasource import *
from yitian.datasource import file_utils, preprocess


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | password      | 'keepsecret'     | SQL password                             |
# | ticker        | ['MSFT']         | the target year for data extraction      |
# | period        | '1d'             | data extraction period                   |
# | table_name    | 'nasdaq_daily'   | the output table in DB                   |
password = locals()['password']
ticker = locals()['ticker']
period = locals()['period']
table_name = locals()['table_name']


# Set up cloud sql connections

connection = pymysql.connect(host=PRIVATE_HOST,
                             user=USER,
                             password=password,
                             db=DATABASE)


# Read In Data

data_pd = yf.download(  # or pdr.get_data_yahoo(...
    # tickers list or string as well
    tickers = [ticker],

    # use "period" instead of start/end
    # valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max
    # (optional, default is '1mo')
    period = period,

    # fetch data by interval (including intraday if period < 60 days)
    # valid intervals: 1m,2m,5m,15m,30m,60m,90m,1h,1d,5d,1wk,1mo,3mo
    # (optional, default is '1d')
    interval = '1d',

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

data_pd = data_pd.reset_index().rename(columns={'Date': DATETIME,
                                                'Open': OPEN,
                                                'High': HIGH,
                                                'Low': LOW,
                                                'Close': CLOSE,
                                                'Volume': VOLUME})

ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True, index_col=DATETIME)
ts_pd = preprocess.add_ymd(ts_pd, index_col=DATETIME)
ts_pd[TICKER] = ticker
ts_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")


# Write historical price to data warehouse by year

for year, grouped_pd in ts_pd.groupby(YEAR):
    bucket_path = file_utils.create_data_path(EQUITY, 'company', ticker.lower(), str(year), 'daily.csv')
    grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')
    print(f"{ticker} in year {year} has been write to {bucket_path}")

