from yitian.datasource import DATE

NAME = 'opec'

OPEC_DATABASE_CODE = "OPEC"

OPEC_DATASET_CODE = "ORB"

# Data pd rename_map
OPEC_PD_RENAME_MAP = {
    'Date': DATE,
    'Value': 'opec_oil_price'
}
