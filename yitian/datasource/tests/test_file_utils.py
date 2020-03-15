import shutil
import tempfile
import unittest
from unittest import mock

from yitian.datasource import *
from yitian.datasource import file_utils


class Test(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._temp_dir)

    def test_create_dw_path(self):

        assert file_utils.create_data_path('commodity/opec', 'crude_oil_price.csv') == \
               'gs://zhongyuan-dw/commodity/opec/crude_oil_price.csv'

        assert file_utils.create_data_path('equity/company', 'appl', str(2020), 'example.csv') == \
               'gs://zhongyuan-dw/equity/company/appl/2020/example.csv'

        assert file_utils.create_data_path('commodity/opec', 'crude_oil_price.csv', base_path=LOCAL_CACHE) == \
                '/home/jupyter/local_cache/commodity/opec/crude_oil_price.csv'

    def test_list_bucket_year_dir(self):

        bucket_parent_dir = 'gs://zhongyuan-dw/commodity/opec'

        assert file_utils.list_bucket_year_dir(bucket_parent_dir, years=[2019, 2020]) == \
               ['gs://zhongyuan-dw/commodity/opec/2019/*', 'gs://zhongyuan-dw/commodity/opec/2020/*']

        assert file_utils.list_bucket_year_dir(bucket_parent_dir, years=[2020], ext='.csv') == \
               ['gs://zhongyuan-dw/commodity/opec/2020/*.csv']
