# logs
ETL_LOG = 'notebooks/etl/etl.log'

# Cloud Storage
DATA_PATH = 'gs://zhongyuan-dw'
LOCAL_CACHE = '/home/jupyter/local_cache'

# Data Category
COMMODITY = 'commodity'
INTEREST_RATE_AND_FIXED_INCOME = 'interest_rate_and_fix_income'
EQUITY = 'equity'

# Cloud SQL
INSTANCE = 'dev-xiangyang'
PRIVATE_HOST= '10.101.224.3'
USER='root'
DATABASE='quant'

# Tables
STOCK_DAILY = 'stock_daily'
STOCK_HOURLY = 'stock_hourly'
STOCK_ACTIONS = 'stock_actions'
MAJOR_HOLDERS= 'major_holders'
INSTITUTIONAL_HOLDERS = 'institutional_holders'
STOCK_RECOMMENDATIONS= 'stock_recommendations'

# Shared Columns Names
TICKER = 'ticker'
DATETIME = 'datetime'
DATE = 'date'
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

# Column Standardization
RAW_DATES = {DATE, 'Date', 'DATE', 'Trade Date'}
