from yitian.datasource import DATE

NAME = 'nasdaq'

NASDAQ_DATABASE_CODE = "NASDAQOMX"

DATASET_CODE_MAP = {
    'settlement_value':        'XQC',
    'national_net_return_idx': 'XNDXT25NNR',
    'net_excess_return_idx':   'XNDXT25NNER',
    'excess_return_idx':       'XNDXT25E',
    'total_return_idx':        'XNDXT25',
}

# Data pd rename_map
NASDAQ_PD_RENAME_MAP = {
    'Trade Date': DATE,
    'Index Value': 'nasdaq_index',
    'High': 'nasdaq_high',
    'Low': 'nasdaq_low',
    'Total Market Value': 'nasdaq_total_market_value',
    'Dividend Market Value': 'nasdaq_dividend_market_value',
}
