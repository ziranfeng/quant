import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from yitian.datasource import *
from yitian.datasource import preprocess


class Test(unittest.TestCase):

    # def test_standardize_date(self):
    #     data_pd = pd.DataFrame([
    #         ['01/01/2019', 11.11],
    #         ['01/04/2019', 44.44],
    #         ['01/03/2019', 33.33],
    #         ['01/02/2019', 22.22]
    #     ], columns=['Trade Date', 'price'])
    #
    #     expect_pd = pd.DataFrame([
    #         ['01/01/2019', 11.11],
    #         ['01/04/2019', 44.44],
    #         ['01/03/2019', 33.33],
    #         ['01/02/2019', 22.22]
    #     ], columns=['date', 'price'])
    #
    #     assert_frame_equal(expect_pd, preprocess.standardize_date(data_pd))
    #
    # def test_standardize_date_with_multi_date_column(self):
    #     data_pd = pd.DataFrame([
    #         ['2019-01-01 00:00:00', '2019-01-01 00:00:00', 11.11],
    #         ['2019-01-02 00:00:00', '2019-01-01 00:00:00', 22.22],
    #         ['2019-01-03 00:00:00', '2019-01-01 00:00:00', 33.33],
    #         ['2019-01-04 00:00:00', '2019-01-01 00:00:00', 44.44],
    #     ], columns=['DATE', 'date', 'price'])
    #
    #     with self.assertRaises(ValueError) as context:
    #         preprocess.standardize_date(data_pd)
    #
    #     assert str(context.exception) == \
    #            str("Original cols ({cols}) cannot be reconnciled with date options ({option})"\
    #                .format(cols=data_pd.columns.tolist(), option=RAW_DATE_OPTIONS))

    def test_create_ts_pd(self):
        data_pd = pd.DataFrame([
            ['01/01/2019', 11.11],
            ['01/04/2019', 44.44],
            ['01/03/2019', 33.33],
            ['01/02/2019', 22.22]
        ], columns=['date', 'price'])

        expect_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01'), 11.11],
            [pd.Timestamp('2019-01-02'), 22.22],
            [pd.Timestamp('2019-01-03'), 33.33],
            [pd.Timestamp('2019-01-04'), 44.44]
        ], columns=['date', 'price']).set_index('date')

        assert_frame_equal(expect_pd, preprocess.create_ts_pd(data_pd))

    def test_create_ts_pd_datetime(self):
        data_pd = pd.DataFrame([
            ['2019-01-01 11:11:11', 11.11],
            ['2019-01-04 04:44:44', 44.44],
            ['2019-01-03 03:33:33', 33.33],
            ['2019-01-02 22:22:22', 22.22]
        ], columns=['datetime', 'price'])

        expect_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01 11:11:11'), 11.11],
            [pd.Timestamp('2019-01-02 22:22:22'), 22.22],
            [pd.Timestamp('2019-01-03 03:33:33'), 33.33],
            [pd.Timestamp('2019-01-04 04:44:44'), 44.44]
        ], columns=['datetime', 'price']).set_index('datetime')

        assert_frame_equal(expect_pd, preprocess.create_ts_pd(data_pd, index_col=DATETIME))

    def test_add_ymd(self):
        data_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01'), 11.11],
            [pd.Timestamp('2019-02-02'), 22.22],
            [pd.Timestamp('2019-03-03'), 33.33],
            [pd.Timestamp('2019-04-04'), 44.44]
        ], columns=['date', 'price']).set_index('date')

        expect_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01'), 11.11, 2019, 1, 1],
            [pd.Timestamp('2019-02-02'), 22.22, 2019, 2, 2],
            [pd.Timestamp('2019-03-03'), 33.33, 2019, 3, 3],
            [pd.Timestamp('2019-04-04'), 44.44, 2019, 4, 4]
        ], columns=['date', 'price', 'year', 'month', 'day']).set_index('date')

        assert_frame_equal(expect_pd, preprocess.add_ymd(data_pd))

    def test_add_ymd_datetime(self):
        data_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01 11:11:11'), 11.11],
            [pd.Timestamp('2019-02-02 22:22:22'), 22.22],
            [pd.Timestamp('2019-03-03 03:33:33'), 33.33],
            [pd.Timestamp('2019-04-04 04:44:44'), 44.44]
        ], columns=['datetime', 'price']).set_index('datetime')

        expect_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01 11:11:11'), 11.11, 2019, 1, 1],
            [pd.Timestamp('2019-02-02 22:22:22'), 22.22, 2019, 2, 2],
            [pd.Timestamp('2019-03-03 03:33:33'), 33.33, 2019, 3, 3],
            [pd.Timestamp('2019-04-04 04:44:44'), 44.44, 2019, 4, 4]
        ], columns=['datetime', 'price', 'year', 'month', 'day']).set_index('datetime')

        assert_frame_equal(expect_pd, preprocess.add_ymd(data_pd, index_col=DATETIME))

    def test_filter_dates(self):
        data_pd = pd.DataFrame([
            [pd.Timestamp('2019-01-01'), 11.11],
            [pd.Timestamp('2019-01-04'), 44.44],
            [pd.Timestamp('2019-01-03'), 33.33],
            [pd.Timestamp('2019-01-02'), 22.22],
            [pd.Timestamp('2019-01-06'), 66.66],
            [pd.Timestamp('2019-01-07'), 77.77],
        ], columns=['date', 'price']).set_index('date')

        expect_pd_one = pd.DataFrame([
            [pd.Timestamp('2019-01-01 00:00:00'), 11.11],
            [pd.Timestamp('2019-01-02 00:00:00'), 22.22],
            [pd.Timestamp('2019-01-03 00:00:00'), 33.33],
            [pd.Timestamp('2019-01-04 00:00:00'), 44.44],
            [pd.Timestamp('2019-01-05 00:00:00'), np.nan],
            [pd.Timestamp('2019-01-06 00:00:00'), 66.66],
            [pd.Timestamp('2019-01-07 00:00:00'), 77.77]
        ], columns=['date', 'price']).set_index('date')


        expect_pd_two = pd.DataFrame([
            [pd.Timestamp('2019-01-04 00:00:00'), 44.44],
            [pd.Timestamp('2019-01-05 00:00:00'), np.nan],
            [pd.Timestamp('2019-01-06 00:00:00'), 66.66],
            [pd.Timestamp('2019-01-07 00:00:00'), 77.77],
            [pd.Timestamp('2019-01-08 00:00:00'), np.nan],
            [pd.Timestamp('2019-01-09 00:00:00'), np.nan]
        ], columns=['date', 'price']).set_index('date')

        assert_frame_equal(expect_pd_one, preprocess.filter_dates(data_pd))
        assert_frame_equal(expect_pd_two,
                           preprocess.filter_dates(data_pd, start_date='2019-01-04', end_date='2019-01-09'))

    def test_find_latest_date(self):
        string_list = ['2018-01-01.pkl',
                       '2019-01-01.pkl',
                       '2019-02-02.pkl',
                       '2019-03-03.pkl',
                       '2019-04-04.pkl']

        assert preprocess.find_latest_date(string_list) == '2018-01-01'
