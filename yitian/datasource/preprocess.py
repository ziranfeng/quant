from typing import List, Dict
import pandas as pd


from yitian.datasource import *


def create_ts_pd(data_pd: pd.DataFrame, standardize_date=True, format=None, sort=True) -> pd.DataFrame:
    """
    Index data_pd by `date`

    :param data_pd: `date` per row
    :param date_col: if not None, rename date_col to `date`
    :param format: data frame timestamp format
    :param sort: sort the index
    """
    ts_pd = data_pd
    if standardize_date:
        date_col = list(set(ts_pd.columns.tolist()).intersection(RAW_DATE_OPTIONS))

        if len(date_col) != 1:
            raise ValueError("columns in dataframe ({cols}) cannot be reconnciled with date options ({option})"
                             .format(cols=ts_pd.columns.tolist(), option=RAW_DATE_OPTIONS))

        ts_pd.rename(index=str, columns={date_col[0]: DATE}, inplace=True)

    ts_pd[DATE] = pd.to_datetime(ts_pd[DATE], cache=True, format=format)
    ts_pd.set_index([DATE], inplace=True)

    if sort:
        ts_pd.sort_index(inplace=True)

    return ts_pd


def add_ymd(ts_pd: pd.DataFrame, sort=True):

    data_pd = ts_pd.reset_index()

    data_pd[YEAR] = pd.DatetimeIndex(data_pd[DATE]).year
    data_pd[MONTH] = pd.DatetimeIndex(data_pd[DATE]).month
    data_pd[DAY] = pd.DatetimeIndex(data_pd[DATE]).day

    data_pd.set_index([DATE], inplace=True)

    if sort:
        data_pd.sort_index(inplace=True)

    return data_pd


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

    return date_range_pd.merge(ts_pd, how='left', on=DATE)
