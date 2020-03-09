import pandas as pd

from yitian.datasource import DATE

def create_index(data_pd: pd.DataFrame, date_col: str, format=None, sort=True):
    """
    Index data_pd by `date`

    :param data_pd: `date` per row
    :param date_col: column name contains data or datetime values
    :param format: dataframe timestamp format
    :param sort: sort the index

    """
    data_pd[DATE] = pd.to_datetime(data_pd[date_col], cache=True, format=format)
    data_pd.set_index([DATE], inplace=True)

    if sort:
        data_pd.sort_index(inplace=True)
