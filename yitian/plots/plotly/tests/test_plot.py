import unittest

import pandas as pd
import pkg_resources

from yitian.plots.plotly import plot

class Test(unittest.TestCase):

    def setUp(self):
        self.ts_pd = pd.DataFrame([
            ['d&j',     pd.Timestamp('2020-02-28'), 25270.830078, 25494.240234, 24681.009766, 25409.359375, 915990000],
            ['d&j',     pd.Timestamp('2020-03-02'), 25590.509766, 26706.169922, 25391.960938, 26703.320313, 637200000],
            ['d&j',     pd.Timestamp('2020-03-03'), 26762.470703, 27084.589844, 25706.279297, 25917.410156, 647080000],
            ['d&j',     pd.Timestamp('2020-03-04'), 26383.679688, 27102.339844, 26286.310547, 27090.859375, 457590000],
            ['d&j',     pd.Timestamp('2020-03-05'), 26671.919922, 26671.919922, 25943.330078, 26121.279297, 477370000],
            ['s&p_500', pd.Timestamp('2020-03-06'), 25457.210938, 25994.380859, 25226.619141, 25864.779297, 599780000],
            ['s&p_500', pd.Timestamp('2020-03-09'), 24992.359375, 24992.359375, 23706.070313, 23851.019531, 750430000],
            ['s&p_500', pd.Timestamp('2020-03-10'), 24453.000000, 25020.990234, 23690.339844, 25018.160156, 654860000],
            ['s&p_500', pd.Timestamp('2020-03-11'), 24604.630859, 24604.630859, 23328.320313, 23553.220703, 663960000],
            ['s&p_500', pd.Timestamp('2020-03-12'), 22184.710938, 22837.949219, 21154.460938, 21200.619141, 908260000],
            ['s&p_500', pd.Timestamp('2020-03-13'), 21973.820313, 23189.759766, 21285.369141, 23185.619141, 843080000]
        ], columns=['index', 'date', 'open', 'high', 'low', 'close', 'volume']).set_index('date')


    def test_time_series(self):

        plot_file = pkg_resources.resource_filename(__name__, 'resources/time_series.html')

        plot.time_series(self.ts_pd, cols=['open', 'high', 'low', 'close'], left_y_title='Dow Jones Index',
                         title='time_series', name='example', x_title='Date Time',
                         right_cols=['volume'], right_y_title='Volume', plot_file=plot_file)

    def test_heatmap(self):
        heatmap_pd =  pd.DataFrame([
            ['dow_jones', pd.Timestamp('2020-02-28'), 1],
            ['dow_jones', pd.Timestamp('2020-03-02'), 0],
            ['dow_jones', pd.Timestamp('2020-03-03'), 1],
            ['dow_jones', pd.Timestamp('2020-03-04'), 1],
            ['dow_jones', pd.Timestamp('2020-03-05'), 0],
            ['s&p_500',   pd.Timestamp('2020-02-28'), 0],
            ['s&p_500',   pd.Timestamp('2020-03-02'), 1],
            ['s&p_500',   pd.Timestamp('2020-03-03'), 0],
            ['s&p_500',   pd.Timestamp('2020-03-04'), 0],
            ['s&p_500',   pd.Timestamp('2020-03-05'), 1],
        ], columns=['index', 'date', 'count'])

        plot_file = pkg_resources.resource_filename(__name__, 'resources/heatmap.html')

        plot.plot_heatmap(heatmap_pd, title='heatmap', name='example',  x_col='date', y_col='index', z_col='count',
                          x_title='dates', y_title='indexes', colorbar_title='records', plot_file=plot_file)

    def test_pie_chart(self):
        grouped_pd =  pd.DataFrame([
            ['dow_jones', 'GOOG', 1],
            ['dow_jones', 'APPL', 2],
            ['dow_jones', 'AMZN', 3],
            ['dow_jones', 'FB',   4],
            ['dow_jones', 'ALBA', 5],
            ['s&p_500',   'GOOG', 9],
            ['s&p_500',   'APPL', 8],
            ['s&p_500',   'AMZN', 7],
            ['s&p_500',   'FB',   6],
            ['s&p_500',   'ALBA', 5],
        ], columns=['index', 'company', 'count'])

        plot_file = pkg_resources.resource_filename(__name__, 'resources/pie_chart.html')

        plot.pie_chart(grouped_pd, label_col='company', value_col='count', group_col='index',
                       title='pie_chart', name='example', plot_file=plot_file)

    def test_plot_histogram(self):

        plot_file = pkg_resources.resource_filename(__name__, 'resources/histogram.html')

        plot.plot_histogram(self.ts_pd, column='close', color='index',
                            title='pie_chart', name='example', plot_file=plot_file)
