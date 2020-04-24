# logs
ETL_LOG = 'logs/etl.log'


# cloud storage
DATA_PATH = 'gs://zhongyuan-dw'
LOCAL_CACHE = '/home/jupyter/local_cache'


# cloud SQL
INSTANCE = 'dev-xiangyang'
PRIVATE_HOST= '10.101.224.3'
USER='ziran'
DATABASE='quant'


# data category / database name
COMMODITY = 'commodity'
INTEREST_RATE_AND_FIXED_INCOME = 'interest_rate_and_fixed_income'
EQUITY = 'equity'


# mysql table name
STOCK_DAILY = 'stock_daily'
STOCK_HOURLY = 'stock_hourly'
STOCK_ACTIONS = 'stock_actions'
MAJOR_HOLDERS= 'major_holders'
INSTITUTIONAL_HOLDERS = 'institutional_holders'
STOCK_RECOMMENDATIONS= 'stock_recommendations'
OPEC_REF_BASKET = 'opec_ref_basket'
FED_SVENPY = 'fed_svenpy'
FED_SVENY = 'fed_sveny'
FED_TIPSY = 'fed_tipsy'
NASDAQ_XQC = 'nasdaq_xqc'
NASDAQ_XNDXT25 = 'nasdaq_xndxt25'
NASDAQ_XNDXT25NNR = 'nasdaq_xndxt25nnr'


# shared column name
TICKER = 'ticker'
DATE = 'date'
DATETIME = 'datetime'
YEAR = 'year'
MONTH = 'month'
DAY = 'day'
UPDATED_AT = 'updated_at'

OPEN = 'open'
HIGH = 'high'
LOW = 'low'
CLOSE = 'close'
VOLUME = 'volume'

DIVIDENDS = 'dividends'
SPLITS = 'splits'


# column standardization
RAW_DATES = {DATE, 'Date', 'DATE', 'Trade Date'}
