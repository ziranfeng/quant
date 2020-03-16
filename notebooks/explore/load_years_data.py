from datetime import datetime as dt

from yitian.datasource import file_utils, load, preprocess


# required parameters
# | parameter     | example          |  description                             |
# |---------------|------------------------------|------------------------------------------|
# | date_range    | ('2017-01-01', '2020-03-13') | the target year for data extraction      |
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

    dir_list = file_utils.list_bucket_year_path(parent_bucket_dir, years=list(range(start_year, end_year + 1)),
                                                ext='.csv')

    data_pd = load.load_lists_of_csv(dir_list)

    ts_pd = preprocess.create_ts_pd(data_pd, standardize_date=True, format=None, sort=True)

    result_pd = preprocess.filter_dates(ts_pd, start_date=date_range[0], end_date=date_range[1])

    name = '_'.join(parent_bucket_dir.split('/')[4:])

    loaded_pds[name] = result_pd

    print("{name} has been added to the data dict".format(name=name))
    print("========================================================")
