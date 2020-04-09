import json
from grpyutil.database import mysql
from grpyutil.database.file import logging


class DataWriter(object):
    def __init__(self, sql_path, copy_tb_conn, copy_tb_name, is_replace=False, write_linenum=30000):

        if sql_path:

            dc = mysql.get_db_conn(sql_path)
            table_name = dc.sql_config["table"]
            self.tc = mysql.TableConn(dc, table_name)

        else:

            copy_tb_definition = copy_tb_conn.read_table_definition()

            tmp_copy_tb_name = copy_tb_conn.get_table_name().upper()

            if not copy_tb_name:

                copy_tb_name = tmp_copy_tb_name + "_COPY"

            if is_replace is True:
                copy_tb_conn.execute("drop table %s" % copy_tb_name)

            tb_definition = copy_tb_definition.replace(tmp_copy_tb_name, copy_tb_name)

            copy_tb_conn.execute(tb_definition)

            # create copy table
            self.tc = copy_tb_conn.copy(copy_tb_name)

        self.write_linenum = write_linenum

        self.cache_write_data_list = []

        self.cache_write_field_list = []

        #
        self.total_write_num = 0

    def set_write_fields(self, field_list):

        self.cache_write_field_list = field_list

    def write_rows(self, value_list, force=False):

        if value_list:
            self.cache_write_data_list.extend(value_list)

        ret = 0

        cache_length = len(self.cache_write_data_list)

        if force or cache_length > self.write_linenum:

            sql = "insert into %s (" % self.tc.get_table_name()
            for field in self.cache_write_field_list:
                sql += "`%s`," % field

            sql = sql[:-1]
            sql = sql + ") values ("
            for field in self.cache_write_field_list:
                sql += "%s,"

            sql = sql[:-1]
            sql = sql + ")"

            ret = self.tc.executemany(sql, self.cache_write_data_list)

            self.tc.commit()

            # print(self.tc.get_sql_path())
            #print(sql, self.cache_write_data_list)

            self.total_write_num = self.total_write_num + cache_length

            self.cache_write_data_list.clear()

        return ret

    def flush(self):

        return self.write_rows(None, force=True)
