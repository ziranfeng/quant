from datetime import datetime as dt
import requests
import pandas as pd

from yitian.datasource import file_utils
from yitian.datasource.quandl import api


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------|------------------------------------------|
# | year          | 2020             | the target year for data extraction      |
# | db_name       | 'NASDAQOMX'      | the data base code from quandl           |
# | ds_name       | 'XQC'            | the data set code from quandl            |
# | output_dw_dir | 'commodity/opec' | the sub-dir in data warehouse for output |
year = locals()['year']
db_name = locals()['db_name']
ds_name = locals()['ds_name']
output_dw_dir = locals()['output_dw_dir']

# ----------------------------------------------------------------------------------------------------------------------
# Set the start and end dates of the selected year

start_date = f'{year}-01-01'
end_date = f'{year}-12-31'

print("The start date is set to {start_date} & The end date is set to {end_date}".format(start_date=start_date,
                                                                                         end_date=end_date))

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
        raise ValueError("User defined start date {start_date} needs to be larger than "
                         "the oldest available date {oldest_available_date}"
                         .format(start_date=start_date, oldest_available_date=oldest_available_date))

if dt.strptime(end_date, "%Y-%m-%d") > dt.strptime(newest_available_date, "%Y-%m-%d"):
    if dt.strptime(end_date, "%Y-%m-%d").year == dt.strptime(newest_available_date, "%Y-%m-%d").year:
        end_date = newest_available_date
    else:
        raise ValueError("User defined end date {end_date} needs to be smaller than "
                         "the newest available date {newest_available_date}"
                         .format(end_date=end_date, newest_available_date=newest_available_date))


# Extract Datasets Data and construct pandas

data_call = api.construct_ts_call(db_name=db_name, ds_name=ds_name, start_date=start_date, end_date=end_date)
extraction = requests.get(data_call).json()

extraction_pd = pd.DataFrame(data=extraction['dataset_data']['data'], columns=column_names)


# Write data to data warehouse

output_file_name = "{start}_{end}_{frequency}.csv".format(start=start_date, end=end_date,
                                                          frequency=frequency)

output_dir = file_utils.create_dw_path(output_dw_dir, str(year), output_file_name)

extraction_pd.to_csv(output_dir, header=True, mode='w', encoding='utf-8')

print("{output_file_name} has been overwrite to {output_dir}"
      .format(output_file_name=output_file_name, output_dir=output_dir))
