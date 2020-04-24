# Extracts, Transform and Load data using 'yfinance' API in batches for historical data and updated data

import logging
from datetime import datetime

from yitian.datasource import *
from yitian.datasource import file_utils, cloud_utils, preprocess, EQUITY
from yitian.datasource.yfinance import api as yf_api

logging.basicConfig(filename=ETL_LOG, level=logging.INFO)


# required parameters
# | parameter       | example          |  description                                                              |
# |-----------------|------------------|---------------------------------------------------------------------------|
# | ticker          | 'MSFT'           | the target year for data extraction                                       |
# | current_year    | 2020             | in non-'update' mode, data of year before 'current_year' will be removed  |
# | mode            | 'update'         | data extraction period                                                    |
ticker = locals()['ticker']
current_year = locals()['current_year']
mode = locals()['mode']


# Set 'daily_period', 'hourly_period' and 'subdir' dates for different mode
if mode == 'update':
    daily_period, hourly_period, subdir = 'ytd', 'ytd', str(current_year)
else:
    daily_period, hourly_period, subdir = '100y', '2y', 'history'

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
    ts_pd[UPDATED_AT] = datetime.now().strftime("%Y-%m-%d")

    # filter out records larger and equal to 'stop_year' if not in `update` mode
    if mode != 'update':
        ts_pd = ts_pd[ts_pd.year < current_year]

    # write batch data price to data storage
    bucket_path = file_utils.create_data_path(EQUITY, 'stocks', ticker.lower(), subdir, 'daily.csv')
    ts_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')

    logging.info(f"{ticker} - daily historical stock data has been write to {bucket_path}")

    # insert batch data price to mysql table
    cloud_utils.csv_to_sql(mysql_instance=INSTANCE, file_path=bucket_path, table=STOCK_DAILY, database=EQUITY)
    logging.info(f"{ticker} - daily historical stock data processing time: {datetime.now() - start}")


except Exception as err:
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
    ts_pd[UPDATED_AT] = datetime.now().strftime("%Y-%m-%d")

    # filter out records larger and equal to 'stop_year' if not in `update` mode
    if mode != 'update':
        ts_pd = ts_pd[ts_pd.year < current_year]

    # write batch data price to data storage
    bucket_path = file_utils.create_data_path(EQUITY, 'stocks', ticker.lower(), subdir, 'hourly.csv')
    ts_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')

    logging.info(f"{ticker} - hourly historical stock data has been write to {bucket_path}")

    # insert batch data price to mysql table
    cloud_utils.csv_to_sql(mysql_instance=INSTANCE, file_path=bucket_path, table=STOCK_HOURLY, database=EQUITY)
    logging.info(f"{ticker} - hourly historical stock data processing time: {datetime.now() - start}")


except Exception as err:
    logging.error(err, exc_info=True)
