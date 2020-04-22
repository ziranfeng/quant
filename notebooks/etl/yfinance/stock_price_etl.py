# This script extract, transform and load stock price for given period & interval using yfinance

import logging
from datetime import datetime as dt

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
period = locals()['password']
interval = locals()['interval']
connection = locals()['connection']

output_file = locals().get('output_file', 'daily.csv')
date_col = locals().get('date_col', 'Date')
instance = locals().get('instance', INSTANCE)
database = locals().get('database', DATABASE)
table = locals().get('table', STOCK_DAILY)

# ====================================================================================================================

try:
    data_pd = yf_api.yfinance_data_api(tickers=[ticker], period=period, interval=interval,
                                       group_by='ticker', auto_adjust=True, prepost=True, threads=True, proxy=None)

    # 60m resolution needs different setting:
    if interval == '60m':
        data_pd[date_col] = [t[:19] for t in data_pd[date_col].astype(str)]

    # standardize col names
    data_pd = data_pd.rename(columns={date_col: DATETIME,
                                      'Open': OPEN,
                                      'High': HIGH,
                                      'Low': LOW,
                                      'Close': CLOSE,
                                      'Volume': VOLUME})

    # transform to ts_pd
    ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True, index_col=DATETIME)
    ts_pd = preprocess.add_ymd(ts_pd, index_col=DATETIME)
    ts_pd[TICKER] = ticker
    ts_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")

    # write batch data price to data storage by year
    for year, grouped_pd in ts_pd.groupby(YEAR):

        bucket_path = file_utils.create_data_path(EQUITY, ticker.lower(), str(year), output_file)

        # Remove the files if the file exists:
        if file_utils.bucket_path_exist(bucket_path):
            file_utils.clean_bucket_path(bucket_path)

            with connection.cursor() as cursor:
                cursor.execute(f" DELETE FROM {table} WHERE ticker='{ticker}' AND year={str(year)};")
                connection.commit()

        grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')
        logging.info(f"{ticker} in year {year} has been write to {bucket_path}")

        cloud_utils.csv_to_sql(mysql_instance=instance, file_path=bucket_path, table=table, database=database)


except Exception as err:
    print(err)
    logging.error(err, exc_info=True)
