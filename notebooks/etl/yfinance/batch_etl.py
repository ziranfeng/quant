# This script extract, transform and load stock price for given period & interval using yfinance

import logging
from datetime import datetime

from yitian.datasource import *
from yitian.datasource import file_utils, cloud_utils, preprocess, EQUITY
from yitian.datasource.yfinance import api as yf_api

logging.basicConfig(filename=ETL_LOG, level=logging.INFO)


# required parameters
# | parameter       | example          |  description                             |
# |-----------------|------------------|------------------------------------------|
# | ticker          | ['MSFT']         | the target year for data extraction      |
# | period_interval | ('max', '1d')    | data extraction period                   |
ticker = locals()['ticker']
mode = locals()['mode']
stop_year = locals()['stop_year']


if mode == 'update':
    daily_period, hourly_period, subdir = 'ytd', 'ytd', stop_year
else:
    daily_period, hourly_period, subdir = 'max', '2y', 'history'

# ====================================================================================================================
# Batch processing daily historical stock data
try:
    start = datetime.now()

    # call yfinance api
    data_pd = yf_api.yfinance_data_api(ticker=ticker, period=daily_period, interval='1d', group_by='ticker',
                                       auto_adjust=True, prepost=True, threads=True, proxy=None)

    # standardize col names
    data_pd = data_pd.rename(columns={'Date': DATETIME, 'Open': OPEN, 'High': HIGH, 'Low': LOW, 'Close': CLOSE, 'Volume': VOLUME})

    # transform to ts_pd
    ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True, index_col=DATETIME)
    ts_pd = preprocess.add_ymd(ts_pd, index_col=DATETIME)
    ts_pd[TICKER] = ticker
    ts_pd[UPDATED_AT] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_pd = ts_pd[[TICKER, DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME, YEAR, MONTH, DAY, UPDATED_AT]]

    # filter out records larger and equal to 'stop_year'
    ts_pd = ts_pd[ts_pd.year < stop_year]

    # write batch data price to data storage
    bucket_path = file_utils.create_data_path(EQUITY, 'stocks', ticker.lower(), subdir, 'daily.py')
    ts_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')

    logging.info(f"{ticker} - daily historical stock data has been write to {bucket_path}")

    # insert batch data price to mysql table
    cloud_utils.csv_to_sql(mysql_instance=INSTANCE, file_path=bucket_path, table=STOCK_DAILY, database=DATABASE)
    logging.info(f"{ticker} - daily historical stock data processing time: {datetime.now() - start}")

except Exception as err:
    print(err)
    logging.error(err, exc_info=True)


# ====================================================================================================================
# Batch processing hourly historical stock data
try:
    start = datetime.now()

    # call yfinance api
    data_pd = yf_api.yfinance_data_api(ticker=ticker, period=hourly_period, interval='60m', group_by='ticker',
                                       auto_adjust=True, prepost=True, threads=True, proxy=None)

    # standardize col names
    data_pd = data_pd.rename(columns={'Datetime': DATETIME, 'Open': OPEN, 'High': HIGH, 'Low': LOW, 'Close': CLOSE, 'Volume': VOLUME})

    # 60m resolution needs different setting:
    data_pd[DATETIME] = [t[:19] for t in data_pd[DATETIME].astype(str)]

    # transform to ts_pd
    ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True, index_col=DATETIME)
    ts_pd = preprocess.add_ymd(ts_pd, index_col=DATETIME)
    ts_pd[TICKER] = ticker
    ts_pd[UPDATED_AT] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ts_pd = ts_pd[[TICKER, DATETIME, OPEN, HIGH, LOW, CLOSE, VOLUME, YEAR, MONTH, DAY, UPDATED_AT]]

    # filter out records larger and equal to 'stop_year'
    ts_pd = ts_pd[ts_pd.year < stop_year]

    # write batch data price to data storage
    bucket_path = file_utils.create_data_path(EQUITY, 'stocks', ticker.lower(), subdir, 'hourly.py')
    ts_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')

    logging.info(f"{ticker} - hourly historical stock data has been write to {bucket_path}")

    # insert batch data price to mysql table
    cloud_utils.csv_to_sql(mysql_instance=INSTANCE, file_path=bucket_path, table=STOCK_HOURLY, database=DATABASE)
    logging.info(f"{ticker} - hourly historical stock data processing time: {datetime.now() - start}")

except Exception as err:
    print(err)
    logging.error(err, exc_info=True)
