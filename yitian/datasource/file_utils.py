import os

import logging
import subprocess
from typing import List

from yitian.datasource import *

log = logging.getLogger(__name__)


def create_data_path(*relative_path, base_path=DATA_PATH) -> str:
    """
    Create path to project data in data warehouse on cloud from relative path components

    :param relative_path: path relative  to data warehouse path
    :param data_warehouse: data warehouse location on cloud

    :return: a full path in data warehouse on cloud
    """

    return '/'.join([base_path] + list(relative_path))


def bucket_to_local(path, cache=LOCAL_CACHE):

    is_relative = not path.startswith('/')

    cache_path = path if is_relative else path[1:]

    local_path = os.path.join(cache, cache_path)

    if os.path.exists(local_path):
        return local_path

    if not os.path.isdir(os.path.dirname(local_path)):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

    bucket_absolute_path = create_data_path(path) if is_relative else path

    cmd = ['gsutil', 'cp', bucket_absolute_path, local_path]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)

    return local_path


def local_to_bucket(path, cache=LOCAL_CACHE):

    if os.path.isabs(path):
        raise ValueError("path ({path}) must be relative to cache ({cache})".format(path=path, cache=cache))

    cache_file = os.path.join(cache, path)
    bucket_path = create_data_path(path)

    cmd = ['gsutil', 'mv', cache_file, bucket_path]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)

    return bucket_path


def list_bucket_year_dir(bucket_parent_dir: str, years: List[int], ext: str=None):

    file_dir_list = ['/'.join([bucket_parent_dir, str(year)]) for year in years]

    if ext:
        return ['/'.join([dir, '*' + ext]) for dir in file_dir_list]
    else:
        return ['/'.join([dir, '*']) for dir in file_dir_list]


def clean_bucket_dir(*relative_path):
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
