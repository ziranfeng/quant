from typing import Tuple

from yitian.datasource import DATA_WAREHOUSE_LOC
from yitian.datasource.quandl import *


def construct_ts_call(db_name: str, ds_name: str, start_date: str, end_date: str, quan_api_https=QUANDL_API_HTTPS,
                      version=QUANDL_API_VERSION, return_format='json', api_key=QUANDL_API_KEY) -> str:

    call = ('{quan_api_https}/{version}/datasets/{db_name}/{ds_name}?'
            'start_date={start_date}&end_date={end_date}&api_key={api_key}') \
        .format(quan_api_https=quan_api_https,
                version=version,
                db_name=db_name,
                ds_name=ds_name,
                return_format=return_format,
                start_date=start_date,
                end_date=end_date,
                api_key=api_key)

    return call


def extraction_output_name_and_dir(extraction, category: str, name: str, subcategory: str, year: int,
                                   data_warehouse=DATA_WAREHOUSE_LOC) -> Tuple[str, str]:

    outfile_name = "{start}_{end}_{frequency}".format(frequency=extraction['dataset']['frequency'],
                                                      start=extraction['dataset']['start_date'],
                                                      end=extraction['dataset']['end_date'])

    output_dir = "{dw_loc}/{category}/{name}/{subcategory}/{year}/{outfile_name}.csv".format(dw_loc=data_warehouse,
                                                                                             category=category,
                                                                                             name=name,
                                                                                             subcategory=subcategory,
                                                                                             year=year,
                                                                                             outfile_name=outfile_name)
    return outfile_name, output_dir
