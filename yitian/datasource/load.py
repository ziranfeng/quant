from typing import List
import pandas as pd


def load_lists_of_csv(file_dir_list: List) -> pd.DataFrame:
    """
    Load a list of csv file with same columns

    :param file_dir_list: list of file dir in cloud bucket

    :return: a concatenated pd dataframe
    """
    files = []
    for file in file_dir_list:
        df = pd.read_csv(file, index_col=None, encoding='utf-8', header=0)
        files.append(df)

    return pd.concat(files, axis=0, ignore_index=True)
