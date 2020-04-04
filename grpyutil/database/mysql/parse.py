import sys
from grpyutil.common.check_rules import Ipv4


def parse_mysql_path(sql_path):
    """
    parse mysql path like : sql://127.0.0.1:3306/user:pwd/database/table
    """
    result_dict = {}

    SQL_PATH_LENGTH_SPECIFY_TABLE = 4

    SQL_PATH_LENGTH_ALL_TABLES = 3

    """
    some may not sure about the suffix
    """
    tmp_path = sql_path
    if tmp_path.endswith("/"):
        tmp_path = tmp_path[:-1]

    # path should start with sql://
    if not tmp_path.startswith("sql://"):
        msg = "sql path is not start with sql:// ,sql_path: %s" % sql_path
        raise Exception(msg)

    # get rid of the prefix sql://
    tmp_path = tmp_path[6:]

    # split the path and parse
    arr = tmp_path.split("/")
    target_len = len(arr)
    if target_len == SQL_PATH_LENGTH_SPECIFY_TABLE:

        result_dict["table"] = arr[3]
    elif target_len == SQL_PATH_LENGTH_ALL_TABLES:

        result_dict["table"] = None
    else:
        msg = "sql_path length is not correct!, sql_path: %s" % sql_path
        raise Exception(msg)

    result_dict["address"] = arr[0]

    addr_arr = arr[0].split(':', 1)
    if len(addr_arr) != 2:
        msg = "sqlpath addr_arr length is not correct, sql_path: %s" % sql_path
        raise Exception(msg)

    result_dict["host"] = addr_arr[0]
    result_dict["port"] = int(addr_arr[1])

    result_dict["host_ipv4"] = Ipv4(result_dict["host"])

    user_pass_arr = arr[1].split(":", 1)
    if len(user_pass_arr) != 2:
        msg = "sqlpath user_pass_arr length is not correct, sql_path: %s" % sql_path
        raise Exception(msg)

    result_dict["user"] = user_pass_arr[0]

    result_dict["password"] = user_pass_arr[1]
    result_dict["database"] = arr[2]

    return result_dict
