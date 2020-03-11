from typing import List, Dict
import pandas as pd


from yitian.datasource import DATE


def create_ts_pd(data_pd: pd.DataFrame, date_col: str=None, format=None, sort=True):
    """
    Index data_pd by `date`

    :param data_pd: `date` per row
    :param date_col: if not None, rename date_col to `date`
    :param format: data frame timestamp format
    :param sort: sort the index
    """
    if date_col:
        data_pd.rename(index=str, columns={date_col: DATE}, inplace=True)

    data_pd[DATE] = pd.to_datetime(data_pd[DATE], cache=True, format=format)
    data_pd.set_index([DATE], inplace=True)

    if sort:
        data_pd.sort_index(inplace=True)


def filter_dates(ts_pd: pd.DataFrame, start_date: str=None, end_date: str=None) -> pd.DataFrame:
    """


    :param ts_pd:
    :param start_date:
    :param end_date:
    :return:
    """
    start_date = ts_pd.index.min() if start_date is None else start_date
    end_date = ts_pd.index.max() if end_date is None else end_date

    date_range_pd = pd.DataFrame(index=pd.date_range(start_date, end_date, name='date'))
    if start_date:
        ts_pd = ts_pd.loc[ts_pd.index >= start_date]
    if end_date:
        ts_pd = ts_pd.loc[ts_pd.index <= end_date]

    return pd.concat([date_range_pd, ts_pd], axis=1, sort=False)
