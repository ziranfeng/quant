from datetime import datetime as dt
import pickle

from yitian.datasource import *
from yitian.datasource import load, preprocess
from yitian.datasource.file_utils import create_data_path, list_bucket_year_path, \
    bucket_to_local, list_bucket_path


# required parameters
# | parameter     | example                      |  description                             |
# |---------------|------------------------------|------------------------------------------|
# | date_range    | ('2017-01-01', '2020-03-13') | the target year for data extraction      |
comp_code = locals()['comp_code']
parent_bucket_list = locals()['parent_bucket_list']
date_range = locals()['date_range']

# ----------------------------------------------------------------------------------------------------------------------

# Read in data from different sources

start_year = dt.strptime(date_range[0], '%Y-%m-%d').year
end_year = dt.strptime(date_range[1], '%Y-%m-%d').year


# Read in data into loaded_pd dict

loaded_pds = {}

for parent_bucket_dir in parent_bucket_list:
    print(parent_bucket_dir)

    dir_list = list_bucket_year_path(parent_bucket_dir, years=list(range(start_year, end_year + 1)), ext='.csv')

    data_pd = load.load_lists_of_csv(dir_list)
    data_pd = preprocess.standardize_date(data_pd, target_date=None)

    ts_pd = preprocess.create_ts_pd(data_pd, format=None, sort=True)

    result_pd = preprocess.filter_dates(ts_pd, start_date=date_range[0], end_date=date_range[1])

    name = '_'.join(parent_bucket_dir.split('/')[4:])

    loaded_pds[name] = result_pd

    print("{name} has been added to the data dict".format(name=name))
    print("========================================================")


# Load Lastest Ticker object

pkl_bucket_dir = create_data_path(EQUITY, 'company', comp_code.lower(), 'pickle')

pickle_list = list_bucket_path(pkl_bucket_dir, ext='.pkl')
latest_version = preprocess.find_latest_date(pickle_list)
local_path = bucket_to_local('/'.join([pkl_bucket_dir, latest_version, '.pkl']))
print("Latest Version on {v} Saved to {path}".format(v=latest_version, path=local_path))

with open(local_path, 'rb') as input:
    company = pickle.load(input)
    print(company.info['longBusinessSummary'])
