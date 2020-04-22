from datetime import datetime as dt
import pandas as pd
import yfinance as yf
import pickle

from yitian.datasource import *
from yitian.datasource import file_utils, preprocess, EQUITY

# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | ticker        | 'MSFT'           | the target year for data extraction      |
connection = locals()['connection']
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
local_path = file_utils.create_local_path(data_category, ticker.lower(), 'pickle', output_file_name)

with open(local_path, 'wb') as output:
    pickle.dump(stock, output, pickle.HIGHEST_PROTOCOL)

file_utils.local_to_bucket(local_path)
print("{output_file_name} has been write to bucket".format(output_file_name=output_file_name))


# stock_actions
action_pd = stock.actions.reset_index().rename(columns={'Date': DATE, 'Dividends': DIVIDENDS, 'Stock Splits': SPLITS})


# stock_holders
columns=['insider', 'inst_s_pct', 'inst_f_pct', 'n_inst']
stock_holder_pd = pd.DataFrame([[num.split('%')[0] for num in stock.major_holders[0].tolist()]], columns=columns)
stock_holder_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d %H:%M:%S")


# institutional_holders
institutional_holders_pd = stock.institutional_holders\
    .rename(columns={'Holder': 'holder', 'Shares': 'shares', 'Date Reported': 'date_reported', '% Out': 'out_pct', 'Value': 'value'})


# recommendations
recommendations_pd = stock.recommendations.reset_index() \
    .rename(columns={'Date': DATE, 'Firm': 'firm', 'To Grade': 'to_grade', 'From Grade': 'from_grade', 'Action': 'action'})


# inert above data into associated sql table
with connection.cursor() as cursor:

    for index, row in action_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, date, dividends, splits) 
        VALUES('{ticker_v}', '{date_v}', '{dividends_v}', '{splits_v}')
        """.format(table_name=STOCK_ACTIONS, ticker_v=ticker, date_v=row[DATE], dividends_v=row[DIVIDENDS], splits_v=row[SPLITS])

        print(sql)
        cursor.execute(sql)
        connection.commit()

    for index, row in stock_holder_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, date_updated, insider_share_pct, institution_share_pct, institution_float_pct, number_institution) 
        VALUES('{ticker}', '{updated_at}', '{insider}', '{inst_s_pct}', '{inst_f_pct}', '{n_inst}')
        """.format(table_name=MAJOR_HOLDERS, ticker=ticker, updated_at=row[UPDATED_AT],
                   insider=row['insider'], inst_s_pct=row['inst_s_pct'], inst_f_pct=row['inst_f_pct'], n_inst=row['n_inst'])

        print(sql)
        cursor.execute(sql)
        connection.commit()

    for index, row in institutional_holders_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, holder, shares, date_reported, out_pct, value) 
        VALUES('{ticker}', '{holder}', '{shares}', '{date_reported}', '{out_pct}', '{value}')
        """.format(table_name=INSTITUTIONAL_HOLDERS, ticker=ticker, holder=row['holder'], shares=row['shares'],
                   date_reported=row['date_reported'], out_pct=row['out_pct'], value=row['value'])

        print(sql)
        cursor.execute(sql)
        connection.commit()

    for index, row in recommendations_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, holder, shares, date_reported, out_pct, value)
        VALUES('{ticker_v}', '{date_v}', {dividends_v}, {splits_v})
        """.format(table_name=STOCK_RECOMMENDATIONS, ticker_v=ticker, date_v=row[DATE], dividends_v=row[OPEN], splits_v=row[HIGH])

        print(sql)
        cursor.execute(sql)
        connection.commit()

