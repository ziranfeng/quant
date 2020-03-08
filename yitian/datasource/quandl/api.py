from yitian.datasource.quandl import *


def construct_metadata_call(db_name: str, ds_name: str, quandl_api_https=QUANDL_API_HTTPS,
                            version=QUANDL_API_VERSION, return_format='json', api_key=QUANDL_API_KEY) -> str:

    metadata_call = ('{quan_api_https}/{version}/datasets/{db_name}/{ds_name}/metadata.{return_format}?'
                     'api_key={api_key}') \
        .format(quan_api_https=quandl_api_https,
                version=version,
                db_name=db_name,
                ds_name=ds_name,
                return_format=return_format,
                api_key=api_key)

    return metadata_call


def construct_ts_call(db_name: str, ds_name: str, start_date: str, end_date: str, quandl_api_https=QUANDL_API_HTTPS,
                      version=QUANDL_API_VERSION, return_format='json', api_key=QUANDL_API_KEY) -> str:

    ts_call = ('{quan_api_https}/{version}/datasets/{db_name}/{ds_name}/data.{return_format}?'
               'start_date={start_date}&end_date={end_date}&api_key={api_key}') \
        .format(quan_api_https=quandl_api_https,
                version=version,
                db_name=db_name,
                ds_name=ds_name,
                return_format=return_format,
                start_date=start_date,
                end_date=end_date,
                api_key=api_key)

    return ts_call
