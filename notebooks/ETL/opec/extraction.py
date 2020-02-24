import requests
import pandas as pd

from yitian.datasource import DATA_WAREHOUSE_LOC
from yitian.datasource.quandl import QUANDL_API_VERSION, QUANDL_API_HTTPS, QUANDL_API_KEY, COMMODITY
from yitian.datasource.quandl.opec import OPEC_DATABASE_CODE, OPEC_DATASET_CODE


# required parameters
start_date = '2000-01-01'
end_date = '2020-01-01'

# optional parameters
return_format = locals().get("return_format", "json")

call = """
{quan_api_https}/{version}/datasets/{db_name}/{ds_name}?start_date={start_date}&end_date={end_date}&api_key={api_key}
""".format(quan_api_https=QUANDL_API_HTTPS,
           version=QUANDL_API_VERSION,
           db_name=OPEC_DATABASE_CODE,
           ds_name=OPEC_DATASET_CODE,
           return_format=return_format,
           start_date=start_date,
           end_date=end_date,
           api_key=QUANDL_API_KEY)

extraction = requests.get(call.replace("\n", "")).json()

opec_pd = pd.DataFrame(data=extraction['dataset']['data'],
                       columns=extraction['dataset']['column_names'])

opec_pd.sort_values('Date', ascending=True, inplace=True)

outfile_name = "opec_crude_oil_{frequency}_{start}_{end}".format(frequency=extraction['dataset']['frequency'],
                                                                 start=extraction['dataset']['start_date'],
                                                                 end=extraction['dataset']['end_date'])
output_dir = "{dw_loc}/{commodity}/opec/{outfile_name}.csv".format(dw_loc=DATA_WAREHOUSE_LOC,
                                                                   commodity=COMMODITY,
                                                                   outfile_name=outfile_name)

example_pd = opec_pd.to_csv(output_dir, header=True, mode='w', encoding='utf-8')
