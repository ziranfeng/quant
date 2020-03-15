import pandas as pd

from yitian.datasource import file_utils, load, preprocess
from yitian.datasource.quandl import nasdaq, opec, fed
from yitian.plots.plotly import plot


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------------------|------------------------------------------|
# | date_range    | ('2017-01-01', '2020-03-13') | the target year for data extraction      |
date_range = locals()['date_range']


# Read in data from different sources

# NASDAQ Settlement Value

nasdaq_settl_dir = 'gs://zhongyuan-dw/equity/nasdaq/settlement_value'
nasdaq_settl_file_ist = file_utils.list_bucket_files(nasdaq_settl_dir)
nasdaq_settlement_pd = load.load_lists_of_csv(nasdaq_settl_file_ist)
preprocess.create_ts_pd(nasdaq_settlement_pd, date_col='Trade Date')


# OPEC Oil Price

opec_dir = 'gs://zhongyuan-dw/commodity/opec'
opec_file_ist = file_utils.list_bucket_files(opec_dir)
opec_pd = load.load_lists_of_csv(opec_file_ist)
preprocess.create_ts_pd(opec_pd, date_col='Date')


# US Fed Base Interest Rate

fed_dir = 'gs://zhongyuan-dw/interest_rate/fed/us_trsy_par_yc'
fed_file_ist = file_utils.list_bucket_files(fed_dir)
fed_pd = load.load_lists_of_csv(fed_file_ist)
preprocess.create_ts_pd(fed_pd, date_col='Date')


# Filter data for specific date range and combine the data

nasdaq_settlement_pd = preprocess.filter_dates(nasdaq_settlement_pd, start_date=date_range[0], end_date=date_range[1])
nasdaq_settlement_pd.rename(columns={'Index Value': 'nasdaq_settlement_value'}, inplace=True)


opec_pd = preprocess.filter_dates(opec_pd, start_date=date_range[0], end_date=date_range[1])
opec_pd.rename(columns={'Value': 'opec_oil_price'}, inplace=True)

fed_pd = preprocess.filter_dates(fed_pd, start_date=date_range[0], end_date=date_range[1])
