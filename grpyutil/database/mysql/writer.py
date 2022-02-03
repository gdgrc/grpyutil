import json
from grpyutil.database import mysql
from grpyutil.database.file import logging
import time

from grpyutil.database.mysql.conn import TableConn


class DataWriter(object):
    def __init__(self, sql_path, copy_tb_conn:TableConn, copy_tb_name, is_replace=False, write_linenum=30000, copy_tb_extra_dml=[],sort=False,stat_time_cost=False):
        self.stat_time_cost= stat_time_cost
        self.sort = sort
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

        self.raw_sql = ""

    def set_write_fields(self, field_list):

        self.cache_write_field_list = field_list

        self.cache_write_on_dup_field_list = field_list

    def set_on_dup_fields(self, field_list):

        self.cache_write_on_dup_field_list = field_list

    def set_ignore(self, b):
        self.ignore = b

    def set_raw_sql(self,b):
        self.raw_sql = b

    def write_rows(self, value_list, force=False,ignore=False):

        if value_list:
            self.cache_write_data_list.extend(value_list)

        ret = 0

        cache_length = len(self.cache_write_data_list)

        if cache_length > 0 and (force or cache_length >= self.write_linenum):
            start = None
            if self.stat_time_cost:
                start = time.time()

            if self.sort:
                pkFields = self.tc.read_pk_fields
                pkIndexList= []
                for pk in pkFields:
                    index = self.cache_write_field_list.index(pk) # this will raise exception
                    # if pk not in self.cache_write_field_list:
                    # raise Exception("pk: %s not in cache_write_field_list: %s" % (pk,self.cache_write_field_list))
                    pkIndexList.append(index)

                if len(pkIndexList)<=0:
                    raise Exception("no pk key")

                def sortFunc(elem):
                    pkCmb = ""
                    for pkIndex in pkIndexList:
                        pkCmb= pkCmb+str(elem[pkIndex])+"&"
                    return pkCmb

                self.cache_write_field_list.sort(key=sortFunc)
 

            sql = self.raw_sql
            if not sql:
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
            execute_ret = None
            commit_ret= None
            while(True):

                try:
                    # print(sql, self.cache_write_data_list)

                    execute_ret = self.tc.executemany(sql, self.cache_write_data_list)  # ret 0

                    commit_ret = self.tc.commit()  # ret 0

                    ret = 1

                    break

                except Exception as e:
                   
                    if "try restarting transaction" in str(e) or "Lost connection" in str(e) or "has gone away" in str(e):
                        logging.info("writeRows retry Exception: %s,try_times: %d",str(e),try_times)
                        time.sleep(2)
                        if try_times > 0:
                            try_times = try_times - 1
                        else:
                            raise Exception("retry %d times,but did not pass,err: %s" % (try_times, e))
                    elif "is full" in str(e):
                        raise Exception(e)
                    else:
                        logging.info("writeRows fatal Exception: %s,try_times: %d,ignore: %s sql: %s demodata: %s(%d)",str(e),try_times,ignore,sql,self.cache_write_data_list[0],len(self.cache_write_data_list[0]))
                        if cache_length<=10:
                            logging.info("fatal sql: %s data: " % (sql,self.cache_write_data_list))
                        if not ignore:
                            raise e
                        break

            # print(self.tc.get_sql_path())
            #print(sql, self.cache_write_data_list)

            self.total_write_num = self.total_write_num + cache_length

            self.cache_write_data_list.clear()

            extra_string = ""

           
            if self.stat_time_cost:
                end = time.time()

                extra_string = end-start

            logging.info("Finishing inserting Table: %s data num: %d.execute_ret: %s,sort: %s. %s" % (self.tc.get_table_name(),cache_length,execute_ret,commit_ret,sort,extra_string))

        return ret

    def flush(self,ignore=False):

        return self.write_rows(None, force=True,ignore=ignore)
