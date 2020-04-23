import logging
import subprocess
from typing import List, Union, Set

from yitian.datasource import *

logging.basicConfig(filename=ETL_LOG, level=logging.INFO)


def csv_to_sql(mysql_instance: str, file_path: str, database: str, table: str):

    cmd = ['gcloud', 'sql', 'import', 'csv', mysql_instance, file_path, f'--database={database}', f'--table={table}']
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
        logging.info(f'{file_path} has been imported to {table} in {database}')
    except subprocess.CalledProcessError as e:
        print(e)
        logging.error(e)
