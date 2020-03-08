from yitian.datasource import DATA_WAREHOUSE_LOC

def create_dw_path(*relative_path, data_warehouse=DATA_WAREHOUSE_LOC) -> str:
    """
    Create path to project data in data warehouse on cloud from relative path components

    :param relative_path: path relative  to data warehouse path
    :param data_warehouse: data warehouse location on cloud

    :return: a full path in data warehouse on cloud
    """

    return '/'.join([data_warehouse] + list(relative_path))
