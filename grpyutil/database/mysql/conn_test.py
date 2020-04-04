import unittest


from grpyutil.database import mysql


if __name__ == "__main__":

    sql_path = mysql.config.sql_path

    tc = mysql.get_table_conn(sql_path)

    print(tc.read_fields())

    print(tc.read_table_definition())

    # print(tc.read_data())

    print(tc.read_table_comment())

    dc = mysql.get_db_conn(sql_path)

    print(dc.travel_tables())
