from datetime import datetime as dt
import logging
import pandas as pd
import yfinance as yf
import pickle

from yitian.datasource import *
from yitian.datasource import file_utils

logging.basicConfig(filename=ETL_LOG, level=logging.INFO)


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | ticker        | 'MSFT'           | the target year for data extraction      |
connection = locals()['connection']
ticker = locals()['ticker']


#-----------------------------------------------------------------------------------------------------------------------
# Consturct stock object using yfiance
stock = yf.Ticker(ticker)


# Write ticket objects by company
output_file_name = '{timestamp}.pkl'.format(timestamp=dt.now().strftime("%Y-%m-%d"))
local_path = file_utils.create_local_path(EQUITY, 'yf_pkl', ticker.lower(), output_file_name)
with open(local_path, 'wb') as output:
    pickle.dump(stock, output, pickle.HIGHEST_PROTOCOL)

cloud_path = file_utils.local_to_bucket(local_path)
logging.info("{ticker} pkl has been write to {cloud_path}".format(ticker=ticker, cloud_path=cloud_path), exc_info=True)


# stock_actions
action_pd = stock.actions.reset_index().rename(columns={'Date': DATE, 'Dividends': DIVIDENDS, 'Stock Splits': SPLITS})


# stock_holders
columns=['insider', 'inst_s_pct', 'inst_f_pct', 'n_inst']
stock_holder_pd = pd.DataFrame([[num.split('%')[0] for num in stock.major_holders[0].tolist()]], columns=columns)
stock_holder_pd[UPDATED_AT] = dt.now().strftime("%Y-%m-%d")


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

        cursor.execute(sql)
        connection.commit()

    for index, row in stock_holder_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, updated_at, insider_share_pct, institution_share_pct, institution_float_pct, number_institution) 
        VALUES('{ticker}', '{updated_at}', '{insider}', '{inst_s_pct}', '{inst_f_pct}', '{n_inst}')
        """.format(table_name=MAJOR_HOLDERS, ticker=ticker, updated_at=row[UPDATED_AT], insider=row['insider'],
                   inst_s_pct=row['inst_s_pct'], inst_f_pct=row['inst_f_pct'], n_inst=row['n_inst'])

        cursor.execute(sql)
        connection.commit()

    for index, row in institutional_holders_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, date_reported, holder, shares, out_pct, value) 
        VALUES('{ticker}', '{date_reported}', '{holder}', '{shares}', '{out_pct}', '{value}')
        """.format(table_name=INSTITUTIONAL_HOLDERS, ticker=ticker, date_reported=row['date_reported'],
                   holder=row['holder'], shares=row['shares'], out_pct=row['out_pct'], value=row['value'])

        cursor.execute(sql)
        connection.commit()

    for index, row in recommendations_pd.iterrows():
        sql = """
        INSERT IGNORE INTO {table_name}(ticker, date, firm, to_grade, from_grade, action)
        VALUES('{ticker}', '{date}', '{firm}', '{to_grade}', '{from_grade}', '{action}')
        """.format(table_name=STOCK_RECOMMENDATIONS, ticker=ticker, date=row[DATE], firm=row['firm'],
                   to_grade=row['to_grade'], from_grade=row['from_grade'], action=row['action'])

        cursor.execute(sql)
        connection.commit()

