from yitian.datasource import *

FED_DATABASE_CODE = "FED"

DATASET_CODE_TABLE_MAP = {
    # 'us_treasury_par_yield_curve'
    'svenpy': FED_SVENPY,

    # 'us_treasury_zero_coupon_yield_curve'
    'sveny': FED_SVENY,

    # 'tips_yield_curve_and_inflation_compensation'
    'tipsy': FED_TIPSY,
}
