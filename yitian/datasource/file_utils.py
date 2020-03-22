import os

import logging
import subprocess
from typing import List, Union

from yitian.datasource import *

log = logging.getLogger(__name__)


def create_data_path(*relative_path, cloud_path=DATA_PATH) -> str:
    """
    Create path in cloud storage

    :param relative_path: path relative to 'base_path'
    :param cloud_path: initial part of cloud path to be be joined with

    :return: a full path in cloud storage
    """
    return '/'.join([cloud_path] + list(relative_path))


def create_local_path(*relative_path, cache=LOCAL_CACHE) -> str:
    """
    Create path in local OS - if not exist, create make directory to the root

    :param relative_path: path relative to 'base_path'
    :param cache: initial part of local path to be be joined with

    :return: a full path in local OS
    """
    local_path = '/'.join([cache] + list(relative_path))

    if not os.path.isdir(os.path.dirname(local_path)):
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

    return local_path


def bucket_to_local(path, cloud_path=DATA_PATH, cache=LOCAL_CACHE) -> str:
    """
    Download object on cloud storage in the mirror directory in local OS

    :param path: absolute path in cloud storage or relative path to 'cloud_path'
    :param base_path: initial part of path in cloud storage
    :param cache: initial part of path in local OS

    :return: an full path in local OS
    """

    is_absolute = path.startswith(cloud_path)

    relative_path = path.split(cloud_path)[1][1:] if is_absolute else path

    local_absolute_path = create_local_path(relative_path, cache=cache)

    if os.path.exists(local_absolute_path):
        log.warning("The path ({path}) exists in local OS".format(path=local_absolute_path))
        return local_absolute_path

    storage_absolute_path = path if is_absolute else create_data_path(relative_path, cloud_path=cloud_path)

    cmd = ['gsutil', 'cp', storage_absolute_path, local_absolute_path]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)

    return local_absolute_path


def local_to_bucket(path, cloud_path=DATA_PATH, cache=LOCAL_CACHE) -> str:
    """
    Push object from local to the mirror directory in cloud storage

    :param path: absolute path in local OS or relative path to 'cache'
    :param cloud_path: initial part of path in cloud storage
    :param cache: initial part of path in local OS

    :return: an full path in cloud storage
    """

    is_absolute = os.path.isabs(path)

    relative_path = path.split(cache)[1][1:] if is_absolute else path

    local_absolute_path = path if is_absolute else create_local_path(path, cache=cache)

    storage_absolute_path = create_data_path(relative_path, cloud_path=cloud_path)

    cmd = ['gsutil', 'mv', local_absolute_path, storage_absolute_path]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)

    return storage_absolute_path


def list_bucket_path(bucket_dir: str, ext: Union[None, str]=None) -> List:
    """
    Generate a list of file paths or sub-directories under a bucket dir

    :param bucket_dir: a dir in cloud bucket
    :param ext: target file extensions, if None return all sub directories

    :return: a list of target file dir in cloud bucket
    """
    cmd = ['gsutil', 'ls', '-r', bucket_dir]
    try:
        path_list = subprocess.check_output(cmd, universal_newlines=True).splitlines()
        if ext:
            path_list = [file_dir for file_dir in path_list if file_dir.endswith(ext)]

        return path_list

    except subprocess.CalledProcessError as e:
        print(e)


def list_bucket_year_path(bucket_parent_dir: str, years: List[int], ext: str=None) -> List:
    """
    Generate a list of file paths or sub-directories under a parent bucket dir for specified years

    :param bucket_parent_dir: a parent dir in cloud bucket
    :param years: years to be included
    :param ext: file extension to be filtered for

    :return: full path of each year
    """
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
    """
    Check whether a full bucket path exist or not

    :param bucket_dir: a parent dir of cloud bucket

    :return: Boolean indicator of path existence
    """

    cmd = ['gsutil', 'ls', '-L', bucket_dir]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
        return True
    except subprocess.CalledProcessError:
        return False


def clean_bucket_path(bucket_dir: str):
    """
    Clean a full bucket path

    :param bucket_dir: a parent dir of cloud bucket
    """
    if bucket_dir.endswith('**'):
        log.warning("Removing whole sub-directory")

    if bucket_dir.endswith('*'):
        log.warning("Removing all objects in a sub-directory")

    cmd = ['gsutil', 'rm', bucket_dir]
    try:
        subprocess.check_call(cmd, stderr=subprocess.STDOUT)
    except subprocess.CalledProcessError as e:
        print(e)
