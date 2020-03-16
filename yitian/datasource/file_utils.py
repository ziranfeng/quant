import os

import logging
import subprocess
from typing import List, Union

from yitian.datasource import *

log = logging.getLogger(__name__)


def create_data_path(*relative_path, base_path=DATA_PATH) -> str:
    """
    Create path to project data in data warehouse on cloud from relative path components

    :param relative_path: path relative to base_path
    :param base_path: initial part of path to be be joined with

    :return: a full path in data warehouse on cloud
    """

    return '/'.join([base_path] + list(relative_path))


def create_local_path(*relative_path, base_path=LOCAL_CACHE) -> str:

    local_path = '/'.join([base_path] + list(relative_path))

    if not os.path.isdir(os.path.dirname(local_path)):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

    return local_path


def bucket_to_local(path, base_path=DATA_PATH) -> str:

    is_absolute = path.startswith(base_path)

    relative_path = path.split(base_path)[1][1:] if is_absolute else path

    local_path = create_local_path(relative_path)

    if os.path.exists(local_path):
        return local_path

    bucket_absolute_path = path if is_absolute else create_data_path(relative_path)

    cmd = ['gsutil', 'cp', bucket_absolute_path, local_path]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)

    return local_path


def local_to_bucket(path, cache=LOCAL_CACHE) -> str:

    is_absolute = os.path.isabs(path)

    cache_file = path if is_absolute else os.path.join(cache, path)

    bucket_path = create_data_path(path.split(cache)[1][1:])

    cmd = ['gsutil', 'mv', cache_file, bucket_path]

    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)

    return bucket_path


def list_bucket_path(bucket_dir: str, ext: Union[None, str]=None) -> List:
    """
    Genrate a list of file paths and sub directories under parent bucket directory

    :param bucket_dir: a parent dir in cloud bucket
    :param ext: target file extensions, if None return all sub directories

    :return: a list of target file dir in cloud bucket
    """
    cmd = ['gsutil', 'ls', '-r', bucket_dir]
    file_list = subprocess.check_output(cmd, universal_newlines=True).splitlines()

    if ext:
        file_list = [file_dir for file_dir in file_list if file_dir.endswith(ext)]

    return file_list


def list_bucket_year_path(bucket_parent_dir: str, years: List[int], ext: str=None) -> List:

    if bucket_parent_dir.endswith('/'):
        log.warning("Improvement: The parent dir shall not end with '/' ")
        bucket_parent_dir = bucket_parent_dir[: -1]

    year_dir_list = ['/'.join([bucket_parent_dir, str(year)]) for year in years]

    file_dir_list = []
    for year_dir in year_dir_list:
        try:
            file_dir_list = file_dir_list + list_bucket_path(year_dir, ext=ext)
        except:
            log.warning("{year_dir} cannot be reached; skipped to process others".format(year_dir=year_dir))
            continue

    return file_dir_list


def bucket_path_exist(bucket_dir: str):

    cmd = ['gsutil', 'ls', '-L', bucket_dir]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError:
        return False


def clean_bucket_path(*relative_path):
    """
    Create path to project data in data warehouse on cloud from relative path components

    :param relative_path: path relative  to data warehouse path
    :param data_warehouse: data warehouse location on cloud

    :return: a full path in data warehouse on cloud
    """
    dir = create_data_path(*relative_path)

    if dir.endswith('**'):
        log.warning("Removing whole sub-directory")

    if dir.endswith('*'):
        log.warning("Removing all objects in a sub-directory")

    cmd = ['gsutil', 'rm', dir]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)
