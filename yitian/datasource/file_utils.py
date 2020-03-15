import logging
import subprocess
from typing import List

from yitian.datasource import DATA_WAREHOUSE_LOC

log = logging.getLogger(__name__)


def create_dw_path(*relative_path, data_warehouse=DATA_WAREHOUSE_LOC) -> str:
    """
    Create path to project data in data warehouse on cloud from relative path components

    :param relative_path: path relative  to data warehouse path
    :param data_warehouse: data warehouse location on cloud

    :return: a full path in data warehouse on cloud
    """

    return '/'.join([data_warehouse] + list(relative_path))


def clean_dw_dir(*relative_path, data_warehouse=DATA_WAREHOUSE_LOC):
    """
    Create path to project data in data warehouse on cloud from relative path components

    :param relative_path: path relative  to data warehouse path
    :param data_warehouse: data warehouse location on cloud

    :return: a full path in data warehouse on cloud
    """
    dir = '/'.join([data_warehouse] + list(relative_path))

    if dir.endswith('**'):
        log.warning("Removing whole sub-directory")

    if dir.endswith('*'):
        log.warning("Removing all objects in a sub-directory")

    cmd = ['gsutil', 'rm', dir]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)


def list_bucket_files(bucket_parent_dir: str, file_ext='.csv') -> List:
    """
    Genrate a list of file dir with specified extention under parent bucket directory

    :param bucket_parent_dir: a parent dir in cloud bucket containing the target files
    :param file_ext: target file extensions

    :return: a list of target file dir in cloud bucket
    """

    cmd = ['gsutil', 'ls', '-r', bucket_parent_dir]
    output = subprocess.check_output(cmd, universal_newlines=True).splitlines()

    # TODO: Due to the unknown issue with gcp storage, gsutil ls commands will return duplicated dir with '//' and '/'
    if bucket_parent_dir.endswith('/'):
        replace_from, replace_to = bucket_parent_dir+'/', bucket_parent_dir
    else:
        replace_from, replace_to = bucket_parent_dir + '//', bucket_parent_dir + '/'

    dedup_sub_dir_list = list(set([dir.replace(replace_from, replace_to) for dir in output]))

    file_dirs = [file_dir for file_dir in dedup_sub_dir_list if file_dir.endswith(file_ext)]

    return file_dirs
