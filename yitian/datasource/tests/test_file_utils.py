import shutil
import tempfile
import unittest
from unittest import mock

from yitian.datasource import file_utils


class Test(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._temp_dir)

    def test_create_dw_path(self):
        assert file_utils.create_dw_path('commodity/opec', 'crude_oil_price.csv') == \
               'gs://zhongyuan-dw/commodity/opec/crude_oil_price.csv'
