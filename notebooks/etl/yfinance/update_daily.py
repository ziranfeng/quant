from datetime import datetime as dt
import pandas as pd
import yfinance as yf
import pickle

from yitian.datasource.yfinance import *
from yitian.datasource import EQUITY, YEAR,  file_utils, preprocess


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | ticker        | 'MSFT'           | the target year for data extraction      |
ticker = locals()['ticker']

# optional parameters
# | parameter     |  description                             |
# |---------------|------------------------------------------|
# | data_category | the general category of the data         |
data_category = locals().get('data_type', EQUITY)
period = locals().get('period', '1d')


#-----------------------------------------------------------------------------------------------------------------------

# Consturct stock object using yfiance
stock = yf.Ticker(ticker)


# Extract historical price

hourly_data = stock.history(period=period, interval='60m').reset_index()
hourly_data['Datetime'] = [pd.Timestamp(dt[:19]) for dt in hourly_data['Datetime'].astype('str')]

daily_data = stock.history(period=period, interval='1d').reset_index()


ts_pd = preprocess.create_ts_pd(data, format=None, sort=True)
output_pd = preprocess.add_ymd(ts_pd)


# Write historical price to data warehouse by year

for year, grouped_pd in output_pd.groupby(YEAR):

    bucket_path = file_utils.create_data_path(data_category, 'company', ticker.lower(), str(year), 'history.csv')
    grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')

    print("{company} in year {year} has been write to {output_dir}".format(company=ticker, year=year, output_dir=bucket_path))


# Write historical price to data warehouse by company

output_file_name = '{timestamp}.pkl'.format(timestamp=dt.now().strftime("%Y-%m-%d"))
local_path = file_utils.create_local_path(data_category, 'company', ticker.lower(), 'pickle', output_file_name)

with open(local_path, 'wb') as output:
    pickle.dump(stock, output, pickle.HIGHEST_PROTOCOL)

file_utils.local_to_bucket(local_path)

print("{output_file_name} has been write to bucket".format(output_file_name=output_file_name))
