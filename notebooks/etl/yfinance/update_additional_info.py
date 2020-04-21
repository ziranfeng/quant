from datetime import datetime as dt
import pandas as pd
import yfinance as yf
import pickle

from yitian.datasource import yfinance
from yitian.datasource import *
from yitian.datasource import file_utils, preprocess


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


#-----------------------------------------------------------------------------------------------------------------------

# Consturct stock object using yfiance
stock = yf.Ticker(ticker)


# Write ticket objects by company
output_file_name = '{timestamp}.pkl'.format(timestamp=dt.now().strftime("%Y-%m-%d"))
local_path = file_utils.create_local_path(data_category, 'company', ticker.lower(), 'pickle', output_file_name)

with open(local_path, 'wb') as output:
    pickle.dump(stock, output, pickle.HIGHEST_PROTOCOL)

file_utils.local_to_bucket(local_path)
print("{output_file_name} has been write to bucket".format(output_file_name=output_file_name))


# stock_actions
action_pd = stock.actions.reset_index() \
    .rename(columns={'Date': DATE,
                     'Dividends': DIVIDENDS,
                     'Stock Splits': SPLITS})

action_pd = preprocess.create_ts_pd(action_pd, format=None, sort=True, index_col=DATE)
action_pd[TICKER] = ticker


# stock_holders
stock_holder_pd = pd.DataFrame([[num.split('%')[0] for num in stock.major_holders[0].tolist()]],
                               columns=['insider_share_pct', 'institution_share_pct', 'institution_float_pct', 'institution_number'])
stock_holder_pd[TICKER] = ticker
stock_holder_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")


# institutional_holders
institutional_holders_pd = stock.institutional_holders \
    .rename(columns={'Holder': 'holder',
                     'Shares': 'shares',
                     'Date Reported': 'date_reported',
                     '% Out': 'out_pct',
                     'Value': 'value'})
institutional_holders_pd[TICKER] = ticker


# recommendations
recommendations_pd = stock.recommendations.reset_index() \
    .rename(columns={'Date': DATE,
                     'Firm': 'firm',
                     'To Grade': 'to_grade',
                     'From Grade': 'from_grade',
                     'Action': 'action'})

