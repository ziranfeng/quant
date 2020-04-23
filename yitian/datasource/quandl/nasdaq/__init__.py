from yitian.datasource import *

NASDAQ_DATABASE_CODE = "NASDAQOMX"

DATASET_CODE_TABLE_MAP = {
    # 'settlement_value'
    'XQC': NASDAQ_XQC,

    # 'total_return_index'
    'XNDXT25': NASDAQ_XNDXT25,

    # 'national_net_return_index'
    'XNDXT25NNR': NASDAQ_XNDXT25NNR,
}
