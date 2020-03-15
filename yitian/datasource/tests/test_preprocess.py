import unittest

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from yitian.datasource import preprocess


class Test(unittest.TestCase):
    
    def test_create_ts_pd(self):
        data_pd_one = pd.DataFrame([
            [pd.Timestamp('2019-01-01 00:00:00'), 11.11],
            [pd.Timestamp('2019-01-04 00:00:00'), 44.44],
            [pd.Timestamp('2019-01-03 00:00:00'), 33.33],
            [pd.Timestamp('2019-01-02 00:00:00'), 22.22],
        ], columns=['date', 'price'])

        expect_pd_one = pd.DataFrame([
            [pd.Timestamp('2019-01-01 00:00:00'), 11.11],
            [pd.Timestamp('2019-01-02 00:00:00'), 22.22],
            [pd.Timestamp('2019-01-03 00:00:00'), 33.33],
            [pd.Timestamp('2019-01-04 00:00:00'), 44.44]
        ], columns=['date', 'price']).set_index('date')

        preprocess.create_ts_pd(data_pd_one, format='%Y-%m-%d')

        data_pd_two = pd.DataFrame([
            ['2019-01-01 00:00:00', 11.11],
            ['2019-01-02 00:00:00', 22.22],
            ['2019-01-03 00:00:00', 33.33],
            ['2019-01-04 00:00:00', 44.44],
        ], columns=['DATE', 'price'])

        expect_pd_two = pd.DataFrame([
            [pd.Timestamp('2019-01-01 00:00:00'), 11.11],
            [pd.Timestamp('2019-01-02 00:00:00'), 22.22],
            [pd.Timestamp('2019-01-03 00:00:00'), 33.33],
            [pd.Timestamp('2019-01-04 00:00:00'), 44.44]
        ], columns=['date', 'price']).set_index('date')

        preprocess.create_ts_pd(data_pd_two, date_col='DATE', format='%Y-%m-%d')

        assert_frame_equal(expect_pd_one, data_pd_one)
        assert_frame_equal(expect_pd_two, data_pd_two)

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
