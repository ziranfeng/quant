from yitian.datasource import DATE

NAME = 'us_fed'

FED_DATABASE_CODE = "FED"

DATASET_CODE_MAP = {
    'us_trsy_par_yc':         'svenpy',
    'us_trsy_zero_coupon_yc': 'sveny',
    'tips_yc_and_infla_comp': 'tipsy',
}

# Data pd rename_map
FED_TRSY_YC_PD_RENAME_MAP = {
    'Date': DATE,
    'SVENPY01': 'fed_01', 'SVENPY02': 'fed_02', 'SVENPY03': 'fed_03', 'SVENPY04': 'fed_04', 'SVENPY05': 'fed_05',
    'SVENPY06': 'fed_06', 'SVENPY07': 'fed_07', 'SVENPY08': 'fed_08', 'SVENPY09': 'fed_09', 'SVENPY10': 'fed_10',
    'SVENPY11': 'fed_11', 'SVENPY12': 'fed_12', 'SVENPY13': 'fed_13', 'SVENPY14': 'fed_14', 'SVENPY15': 'fed_15',
    'SVENPY16': 'fed_16', 'SVENPY17': 'fed_17', 'SVENPY18': 'fed_18', 'SVENPY19': 'fed_19', 'SVENPY20': 'fed_20',
    'SVENPY21': 'fed_21', 'SVENPY22': 'fed_22', 'SVENPY23': 'fed_23', 'SVENPY24': 'fed_24', 'SVENPY25': 'fed_25',
    'SVENPY26': 'fed_26', 'SVENPY27': 'fed_27', 'SVENPY28': 'fed_28', 'SVENPY29': 'fed_29', 'SVENPY30': 'fed_30',
}
