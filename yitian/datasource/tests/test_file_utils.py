import os
import shutil
import tempfile
import unittest
from unittest import mock

from yitian.datasource import *
from yitian.datasource import file_utils, load, preprocess


class Test(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._temp_dir)

    def test_create_data_path(self):
        # Test default base path
        assert file_utils.create_data_path('equity/company', 'appl', str(2020), 'example.csv') == \
               'gs://zhongyuan-dw/equity/company/appl/2020/example.csv'

        # Test customized base path
        assert file_utils.create_data_path('commodity/opec', 'crude_oil_price.csv', base_path='gs://dw') == \
                'gs://dw/commodity/opec/crude_oil_price.csv'

    @mock.patch('os.makedirs')
    def test_create_local_path(self, mock_makedirs):
        # Test default base path

        os.makedirs(os.path.dirname('/home/jupyter/local_cache/equity/company/appl/2020/example.csv'), exist_ok=True)

        mock_makedirs.assert_called_once()
        mock_makedirs.assert_called_with('/home/jupyter/local_cache/equity/company/appl/2020', exist_ok=True)

    def test_list_bucket_year_path(self):

        bucket_parent_dir = 'gs://zhongyuan-dw/commodity/opec'

