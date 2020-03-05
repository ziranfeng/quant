import requests
import pandas as pd

from yitian.datasource.quandl import *
from yitian.datasource.quandl import api, opec, nasdaq


# required parameters
# | parameter  | example  |  description                        |
# |------------|----------|-------------------------------------|
# | year       | 2020     | the target year for data extraction |
year = 2020

# ----------------------------------------------------------------------------------------------------------------------

# Set the start and end dates of the selected year

start_date = f'{year}-01-01'
end_date = f'{year}-12-31'


# Extract OPEC Reference Basket Price data

call = api.construct_ts_call(db_name=opec.OPEC_DATABASE_CODE, ds_name=opec.OPEC_DATASET_CODE,
                             start_date=start_date, end_date=end_date)

extraction = requests.get(call).json()

opec_pd = pd.DataFrame(data=extraction['dataset']['data'], columns=extraction['dataset']['column_names'])

_, output_dir = api.extraction_output_name_and_dir(extraction=extraction, category=COMMODITY,
                                                              name=opec.NAME, subcategory='crude_oil_price', year=year)

opec_pd.to_csv(output_dir, header=True, mode='w', encoding='utf-8')


# Extract OPEC Reference Basket Price data

for subcategory, dataset_name in nasdaq.DATASET_CODE_MAP.items():

    call = api.construct_ts_call(db_name=nasdaq.NASDAQ_DATABASE_CODE, ds_name=dataset_name,
                                 start_date=start_date, end_date=end_date)

    extraction = requests.get(call).json()

    nasdaq_pd = pd.DataFrame(data=extraction['dataset']['data'], columns=extraction['dataset']['column_names'])

    _, output_dir = api.extraction_output_name_and_dir(extraction=extraction, category=EQUITY,
                                                                  name=nasdaq.NAME, subcategory=subcategory, year=year)

    nasdaq_pd.to_csv(output_dir, header=True, mode='w', encoding='utf-8')
