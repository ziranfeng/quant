import requests

from yitian.datasource.quandl import QUANDL_API_VERSION, QUANDL_API_HTTPS, QUANDL_API_KEY
from yitian.datasource.quandl.opec import OPEC_DATABASE_CODE, OPEC_DATASET_CODE


# required parameters
start_date = '2000-01-01'
end_date = '2020-01-01'

# optional parameters
return_format = locals().get("return_format", "json")


call = """
{quan_api_https}/{version}/datasets/{db_name}/{ds_name}/data.{return_format}?start_date={start_date}&end_date={end_date}&api_key={api_key}
""".format(quan_api_https=QUANDL_API_HTTPS,
           version=QUANDL_API_VERSION,
           db_name=OPEC_DATABASE_CODE,
           ds_name=OPEC_DATASET_CODE,
           return_format=return_format,
           start_date=start_date,
           end_date=end_date,
           api_key=QUANDL_API_KEY)

extraction = requests.get(call.replace("\n", "")).json()
