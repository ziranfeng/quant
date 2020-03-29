from typing import List, Dict
import re
from datetime import datetime as dt
import pandas as pd


from yitian.datasource import *


def create_ts_pd(data_pd: pd.DataFrame, format=None, sort=True) -> pd.DataFrame:
    """
    Index data_pd by `date`

    :param data_pd: `date` or optional 'date' per row
    :param format: data frame timestamp format
    :param sort: sort the index

    :return: a data frame indexed by `date`
    """
    if DATETIME not in data_pd.columns:
        raise ValueError(f"{DATETIME} is not appeared in ({data_pd.columns})")

    data_pd[DATETIME] = pd.to_datetime(data_pd[DATETIME], cache=True, format=format)
    data_pd.set_index([DATETIME], inplace=True)

    if sort:
        data_pd.sort_index(inplace=True)

    return data_pd


def add_ymd(ts_pd: pd.DataFrame, sort=True):
    """
    Add `year`, `month` and `day` to ts_pd

    :param ts_pd: pd indexed by `date`
    :param sort: sort the index

    :return: ts_pd with additional columns `year`, `month` and `day`
    """

    data_pd = ts_pd.reset_index()

    data_pd[YEAR] = pd.DatetimeIndex(data_pd[DATETIME]).year
    data_pd[MONTH] = pd.DatetimeIndex(data_pd[DATETIME]).month
    data_pd[DAY] = pd.DatetimeIndex(data_pd[DATETIME]).day

    data_pd.set_index([DATETIME], inplace=True)

    if sort:
        data_pd.sort_index(inplace=True)

    return data_pd


def filter_dates(ts_pd: pd.DataFrame, start_date: str=None, end_date: str=None) -> pd.DataFrame:
    """
    Filter ts_pd on date range and fill the missing dates with `Null`

    :param ts_pd: ts_pd indexed by `date`
    :param start_date: start date
    :param end_date: end date

    :return: filtered ts_pd
    """
    start_date = ts_pd.index.min() if start_date is None else start_date
    end_date = ts_pd.index.max() if end_date is None else end_date

    date_range_pd = pd.DataFrame(index=pd.date_range(start_date, end_date, name='date'))
    ts_pd = ts_pd.loc[(ts_pd.index >= start_date) & (ts_pd.index <= end_date)]

    return date_range_pd.merge(ts_pd, how='left', on=DATE)


def find_latest_date(string_list: List) -> str:

    date_list = [_extract_date(s) for s in string_list]
    min_date = pd.to_datetime(date_list, format='%Y-%m-%d').min().date()

    return str(min_date)


def _extract_date(string: str):
    return re.search('\d{4}-\d{2}-\d{2}', string).group()
