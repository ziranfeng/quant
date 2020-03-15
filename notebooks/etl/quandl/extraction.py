from datetime import datetime as dt
import requests
import pandas as pd

from yitian.datasource import file_utils
from yitian.datasource.quandl import api


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | data_category | 'equity'         | the general category of the data         |
# | year          | 2020             | the target year for data extraction      |
# | db_name       | 'NASDAQOMX'      | the data base code from quandl           |
# | ds_name       | 'XQC'            | the data set code from quandl            |
data_category = locals()['data_category']
year = locals()['year']
db_name = locals()['db_name']
ds_name = locals()['ds_name']

# ----------------------------------------------------------------------------------------------------------------------

# Set the start and end dates of the selected year

start_date, end_date = f'{year}-01-01', f'{year}-12-31'
print("Start date ({start_date}) & End date ({end_date})".format(start_date=start_date, end_date=end_date))


# Extract Datasets Metadata

metadata_call = api.construct_metadata_call(db_name=db_name, ds_name=ds_name)
metadata = requests.get(metadata_call).json()

locals().update(metadata['dataset'])
print(*metadata['dataset'].items(), sep='\n')


# Check start and end dates within range

if dt.strptime(start_date, "%Y-%m-%d") < dt.strptime(oldest_available_date, "%Y-%m-%d"):
    if dt.strptime(start_date, "%Y-%m-%d").year == dt.strptime(oldest_available_date, "%Y-%m-%d").year:
        start_date = oldest_available_date
    else:
        raise ValueError("Start date ({start_date}) needs to be larger than the oldest available date ({new_start_date})"
                         .format(start_date=start_date, new_start_date=oldest_available_date))

if dt.strptime(end_date, "%Y-%m-%d") > dt.strptime(newest_available_date, "%Y-%m-%d"):
    if dt.strptime(end_date, "%Y-%m-%d").year == dt.strptime(newest_available_date, "%Y-%m-%d").year:
        end_date = newest_available_date
    else:
        raise ValueError("End date ({end_date}) needs to be smaller than the newest available date ({new_end_date})"
                         .format(end_date=end_date, new_end_date=newest_available_date))


# Extract Datasets Data and construct pandas

data_call = api.construct_ts_call(db_name=db_name, ds_name=ds_name, start_date=start_date, end_date=end_date)
extraction = requests.get(data_call).json()

extraction_pd = pd.DataFrame(data=extraction['dataset_data']['data'], columns=column_names)


# Define output dir names and Write data to data warehouse

output_dir = file_utils.create_data_path(data_category, db_name.lower(), ds_name.lower(), str(year), 'history.csv')

extraction_pd.to_csv(output_dir, header=True, index=False, mode='w', encoding='utf-8')

print("{db_name} / {ds_name} in {year} has been overwrite to {output_dir}"
      .format(db_name=db_name, ds_name=ds_name, year=year, output_dir=output_dir))
