import shutil
import tempfile
import unittest
from unittest import mock


from yitian.datasource.quandl import api, QUANDL_API_HTTPS, QUANDL_API_VERSION, QUANDL_API_KEY


class Test(unittest.TestCase):

    def setUp(self):
        self._temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self._temp_dir)

    def test_construct_metadata_call(self):
        assert api.construct_metadata_call(db_name='TEST_DB', ds_name='TEST_DS') == \
               '{quan_api_https}/{version}/datasets/TEST_DB/TEST_DS/metadata.json?api_key={api_key}' \
                   .format(quan_api_https=QUANDL_API_HTTPS,
                           version=QUANDL_API_VERSION,
                           api_key=QUANDL_API_KEY)

    def test_construct_ts_call(self):
        assert api.construct_ts_call(db_name='TEST_DB', ds_name='TEST_DS', start_date='1111-11-11',
                                     end_date='2222-22-22') == \
               ('{quan_api_https}/{version}/datasets/TEST_DB/TEST_DS/data.json?'
                'start_date=1111-11-11&end_date=2222-22-22&api_key={api_key}').format(quan_api_https=QUANDL_API_HTTPS,
                                                                                      version=QUANDL_API_VERSION,
                                                                                      api_key=QUANDL_API_KEY)
