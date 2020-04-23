# Extracts, Transform and Load data using quandl API in batches for historical data and updated data

import logging
import requests
import pandas as pd

from yitian.datasource import *
from yitian.datasource import file_utils, cloud_utils
from yitian.datasource.quandl import api

logging.basicConfig(filename=ETL_LOG, level=logging.INFO)


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | db_name       | 'NASDAQOMX'      | the data base code from quandl           |
# | ds_name       | 'XQC'            | the data set code from quandl            |
# | current_year  | 2020             | the target year for data extraction      |
# | mode          | 'update'         | data extraction period                   |
# | data_category | 'equity'         | the general category of the data         |
# | output_table  | 'nasdaq_xqc'     | the table name in MySQL                  |

db_name = locals()['db_name']
ds_name = locals()['ds_name']
current_year = locals()['current_year']
mode = locals()['mode']
data_category = locals()['data_category']
output_table = locals()['output_table']


# ----------------------------------------------------------------------------------------------------------------------
try:
    # Extract Datasets Metadata & localize variables
    metadata_call = api.construct_metadata_call(db_name=db_name, ds_name=ds_name)
    metadata = requests.get(metadata_call).json()
    locals().update(metadata['dataset'])

    # Set 'start_date', 'end_date' and 'dir' dates for different mode
    if mode == 'update':
        start_date, end_date, dir = f'{str(current_year)}-01-01', newest_available_date, f'{str(current_year)}'
    else:
        start_date, end_date, dir = oldest_available_date, f'{str(current_year-1)}-12-31', 'history'

    logging.info(f"start date: {start_date} & end date: {end_date}", exc_info=True)


    # Extract Datasets data
    data_call = api.construct_ts_call(db_name=db_name, ds_name=ds_name, start_date=start_date, end_date=end_date)
    extraction = requests.get(data_call).json()

    # Transform data to pandas with standardized column names
    standardized_column_names = [name.replace(' ', '_').lower() for name in column_names]
    extraction_pd = pd.DataFrame(data=extraction['dataset_data']['data'], columns=standardized_column_names)

    # Define output dir names and Write data to data warehouse
    bucket_path = file_utils.create_data_path(data_category, db_name.lower(), ds_name.lower(), dir, 'data.csv')
    extraction_pd.to_csv(bucket_path, header=True, index=False, mode='w', encoding='utf-8')

    # Load data into mysql table
    cloud_utils.csv_to_sql(mysql_instance=INSTANCE, file_path=bucket_path, database=data_category, table=output_table)

    logging.info(f"{db_name} / {ds_name} has been overwrite to {bucket_path}", exc_info=True)


except Exception as err:
    print(err)
    logging.error(err, exc_info=True)
