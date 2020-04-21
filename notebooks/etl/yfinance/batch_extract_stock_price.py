# This script extract stock price for given period & interval
# and write to cloud storage

import logging
from datetime import datetime as dt
import yfinance as yf

from yitian.datasource import *
from yitian.datasource import file_utils, preprocess


# required parameters
# | parameter       | example          |  description                             |
# |-----------------|------------------|------------------------------------------|
# | ticker          | ['MSFT']         | the target year for data extraction      |
# | period_interval | ('max', '1d')    | data extraction period                   |
ticker = locals()['ticker']
period_interval = locals().get('period_interval', ('max', '1d'))

# --------------------------------------------------------------------------------------------------------------------

period, interval = period_interval
if interval == '60m' and period in ['5y', '10y', 'max']:
    raise ValueError(f"When Interbal is ({interval}), period can only be set to maxmium '2y' ")

def format_ts_pd(ts_pd):
    ts_pd = preprocess.create_ts_pd(ts_pd, format=None, sort=True, index_col=DATETIME)
    ts_pd = preprocess.add_ymd(ts_pd, index_col=DATETIME)
    ts_pd[TICKER] = ticker
    ts_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    return ts_pd

# ====================================================================================================================

if interval=='60m':
    output_file = 'hourly.csv'
    original_date_col = 'Datetime'
else:
    output_file = 'daily.csv'
    original_date_col = 'Date'


# extract daily batch data
try:
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

    data_pd = data_pd.reset_index() \
        .rename(columns={original_date_col: DATETIME,
                         'Open': OPEN,
                         'High': HIGH,
                         'Low': LOW,
                         'Close': CLOSE,
                         'Volume': VOLUME})

    # The `datetime` in 60m file needs to be parsed:
    if interval == '60m':
        data_pd[DATETIME] = [t[:19] for t in data_pd[DATETIME].astype(str)]

    ts_pd = format_ts_pd(data_pd)

    # write batch data price to data storage by year
    for year, grouped_pd in ts_pd.groupby(YEAR):
        bucket_path = file_utils.create_data_path(EQUITY, 'company', ticker.lower(), str(year), output_file)
        grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')
        print(f"{ticker} in year {year} has been write to {bucket_path}")


except Exception as err:
    logging.warning(err, exc_info=True)
