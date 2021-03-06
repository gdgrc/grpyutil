import json
from grpyutil.database import mysql
from grpyutil.database.file import logging
import time


class DataWriter(object):
    def __init__(self, sql_path, copy_tb_conn, copy_tb_name, is_replace=False, write_linenum=30000, copy_tb_extra_dml=[]):

        if sql_path:

            dc = mysql.get_db_conn(sql_path)
            table_name = dc.sql_config["table"]
            self.tc = mysql.TableConn(dc, table_name)

        else:

            copy_tb_definition = copy_tb_conn.read_table_raw_definition()

            tmp_copy_tb_name = copy_tb_conn.get_table_name()

            if not copy_tb_name:

                copy_tb_name = tmp_copy_tb_name + "_COPY"

            if is_replace is True:
                copy_tb_conn.execute("drop table if exists %s" % copy_tb_name)

            tb_definition = copy_tb_definition.replace(tmp_copy_tb_name, copy_tb_name)

            # print(copy_tb_name, tb_definition)
            try:
                copy_tb_conn.execute(tb_definition)
            except Exception as e:
                print("sql: ",tb_definition," err: ",e)

            if copy_tb_extra_dml:
                for dml in copy_tb_extra_dml:
                    try:
                        copy_tb_conn.execute(dml % (copy_tb_name))
                    except Exception as e:
                        raise Exception(e)

            # create copy table
            self.tc = copy_tb_conn.copy(copy_tb_name)

        self.write_linenum = write_linenum

        self.cache_write_data_list = []

        self.cache_write_field_list = []

        self.cache_write_on_dup_field_list = []

        #
        self.ignore = False

        self.total_write_num = 0

    def set_write_fields(self, field_list):

        self.cache_write_field_list = field_list

        self.cache_write_on_dup_field_list = field_list

    def set_on_dup_fields(self, field_list):

        self.cache_write_on_dup_field_list = field_list

    def set_ignore(self, b):
        self.ignore = b

    def write_rows(self, value_list, force=False):

        if value_list:
            self.cache_write_data_list.extend(value_list)

        ret = 0

        cache_length = len(self.cache_write_data_list)

        if cache_length > 0 and (force or cache_length >= self.write_linenum):
            sql = "insert "
            if self.ignore:
                sql += " ignore "
            sql += " into %s (" % self.tc.get_table_name()
            for field in self.cache_write_field_list:
                sql += "`%s`," % field

            sql = sql[:-1]
            sql = sql + ") values ("
            for field in self.cache_write_field_list:
                sql += "%s,"

            sql = sql[:-1]
            sql = sql + ") ON DUPLICATE KEY UPDATE "

            for field in self.cache_write_on_dup_field_list:
                sql += "`%s`=VALUES(`%s`)," % (field, field)

            sql = sql[:-1]

            # try to retry the transaction

            try_times = 1000
            while(True):

                try:
                    # print(sql, self.cache_write_data_list)

                    self.tc.executemany(sql, self.cache_write_data_list)  # ret 0

                    self.tc.commit()  # ret 0

                    ret = 1

                    break

                except Exception as e:
                    if "try restarting transaction" in str(e):

                        time.sleep(2)
                        if try_times > 0:
                            try_times = try_times - 1
                        else:
                            raise Exception("retry %d times,but did not pass,err: %s" % (try_times, e))
                    else:
                        raise e

            # print(self.tc.get_sql_path())
            #print(sql, self.cache_write_data_list)

            self.total_write_num = self.total_write_num + cache_length

            self.cache_write_data_list.clear()

            logging.info("Finishing inserting data num: %d" % cache_length)

        return ret

    def flush(self):

        return self.write_rows(None, force=True)
