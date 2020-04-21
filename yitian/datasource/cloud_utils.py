import os

import logging
import subprocess
from typing import List, Union

from yitian.datasource import *

log = logging.getLogger(__name__)


def csv_to_mysql(mysql_instance: str, file_path: str, table: str, database: str=DATABASE):

    cmd = ['gcloud', 'sql', 'import', 'csv', mysql_instance, file_path, f'--database={database}', f'--table={table}']
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)
