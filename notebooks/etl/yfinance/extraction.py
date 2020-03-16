from datetime import datetime as dt
import yfinance as yf
import pickle

from yitian.datasource import *
from yitian.datasource import file_utils, preprocess


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | comp_code     | 'MSFT'           | the target year for data extraction      |
comp_code = locals()['comp_code']

# optional parameters
# | parameter     |  description                             |
# |---------------|------------------------------------------|
# | data_category | the general category of the data         |
data_category = locals().get('data_type', EQUITY)

#-----------------------------------------------------------------------------------------------------------------------

# Consturct stock object using yfiance

stock = yf.Ticker(comp_code)


# Extract historical price

data = stock.history(period="max").reset_index()
preprocess.create_ts_pd(data, standardize_date=True, format=None, sort=True)
output_pd = preprocess.add_ymd(data)


# Write historical price to data warehouse by year

for year, grouped_pd in output_pd.groupby(YEAR):

    bucket_path = file_utils.create_data_path(data_category, 'company', comp_code.lower(), str(year), 'history.csv')
    grouped_pd.to_csv(bucket_path, header=True, index=True, mode='w', encoding='utf-8')

    print("{company} in year {year} has been write to {output_dir}"
          .format(company=comp_code, year=year, output_dir=bucket_path))


# Write historical price to data warehouse by company

output_file_name = '{timestamp}.pkl'.format(timestamp=dt.now().strftime("%Y-%m-%d"))
local_path = file_utils.create_local_path(data_category, 'company', comp_code.lower(), 'pickle', output_file_name)

with open(local_path, 'wb') as output:
    pickle.dump(stock, output, pickle.HIGHEST_PROTOCOL)

file_utils.local_to_bucket(local_path)

print("{output_file_name} has been write to bucket".format(output_file_name=output_file_name))
