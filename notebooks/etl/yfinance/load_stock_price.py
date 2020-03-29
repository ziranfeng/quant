from datetime import datetime as dt
import pandas as pd
import pymysql
import yfinance as yf

from yitian.datasource import HOST, USER, DATABASE, EQUITY, DATETIME, YEAR, UPDATED_AT, file_utils, preprocess
from yitian.datasource.yfinance import *


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | password      | 'keepsecret'     | SQL password                             |
# | ticker        | ['MSFT']         | the target year for data extraction      |
# | table_name    | 'nasdaq_daily'   | the output table in DB                   |
password = locals()['password']
ticker = locals()['ticker']
table_name = locals()['table_name']


# optional parameters
# | parameter     |  description                             |
# |---------------|------------------------------------------|
# | data_category | the general category of the data         |
period = locals().get('period', '1d')
interval = locals().get('period', '1d')


if interval=='60m':
    period ='2y' if period.isin(['5y', '10y', 'max']) else period


# Set up cloud sql connections
connection = pymysql.connect(host=HOST,
                             user=USER,
                             password=password,
                             db=DATABASE)


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
    interval = interval,

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

if interval=='60m':
    data_pd['Datetime'] = [pd.Timestamp(dt[:19]) for dt in data_pd['Datetime'].astype('str')]


data_pd.rename(columns={'Datetime': DATETIME,
                        'Open': OPEN,
                        'High': HIGH,
                        'Low': LOW,
                        'Close': CLOSE,
                        'Volume': VOLUME},
               inplace=True)


# Standardize data_pd
ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True)
ts_pd = preprocess.add_ymd(ts_pd)
ts_pd[TICKER] = ticker
ts_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")


# Write historical price to data warehouse by year
for year, grouped_pd in ts_pd.groupby(YEAR):
    bucket_path = file_utils.create_data_path(EQUITY, 'company', ticker.lower(), str(year), f'{interval}.csv')
    grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')
    print(f"{ticker} in year {year} has been write to {bucket_path}")


try:
    sql_pd = ts_pd.reset_index()
    sql_pd.to_sql(name=table_name, con=connection, if_exists='append')

except ValueError as e:
    print(e)
