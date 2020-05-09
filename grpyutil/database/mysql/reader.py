import json
from grpyutil.database import mysql
from grpyutil.database.file import logging


class DataReader(object):
    def __init__(self, begin_index, begin_id_index, sql_path, finish_flag, end_read_num, begin_id_index_name, dc=None, table_name=None, read_linenum=30000):
        self.begin_index = begin_index
        self.begin_id_index = begin_id_index
        self.begin_id_index_name = begin_id_index_name

        self.finish_flag = finish_flag
        self.end_read_num = end_read_num
        self.type = 0

        if sql_path:
            self.sql_path = sql_path
            self.dc = mysql.get_db_conn(sql_path)
            table_name = self.dc.sql_config["table"]
            self.tc = mysql.TableConn(self.dc, table_name)
        else:
            self.dc = dc

            self.tc = mysql.TableConn(self.dc, table_name)

        self.read_linenum = read_linenum

        # reading index
        self.read_id_index = self.begin_id_index
        self.read_index = self.begin_index

        self.cache_data = None

        #
        self.total_read_num = 0

        self.get_fields_index_dict()

        if self.begin_id_index_name:
            # check if this is a index

            assert self.tc.check_if_column_is_index(self.begin_id_index_name), "%s is not a index!" % (self.begin_id_index_name)

    def get_tb_conn(self):
        return self.tc

    def load(self):
        # loads and write into file like json.load loads
        pass

    def loads(self):
        pass

    def set_finish_flag(self, finish_flag):
        self.finish_flag = finish_flag

    def get_index_by_field_name(self, field_name):

        return self.fields_index_dict[field_name]

    def get_fields_index_dict(self):
        self.fields_index_dict = {}
        self.fields_list = self.get_row_fields()

        for i in range(len(self.fields_list)):
            field = self.fields_list[i]
            self.fields_index_dict[field] = i

    def set_filter_fields(self, filter_fields, force=False):
        self.tc.set_filter_fields(filter_fields, force)
        self.get_fields_index_dict()

    def get_row_fields(self):
        return self.tc.read_data_columns()

    def log_dumps(self):
        return "%d %s %s %s %s %s\n" % (self.finish_flag,
                                        self.sql_path, self.begin_index, self.begin_id_index, self.end_read_num, self.type)

    def get_next_rows(self, dict_query=False, extra_sql=""):
        # begin to read data

        if self.end_read_num and self.total_read_num >= self.end_read_num:

            return None, 0

        read_linenum = self.read_linenum
        if self.end_read_num and read_linenum > self.end_read_num - self.total_read_num:
            read_linenum = self.end_read_num - self.total_read_num

        rows_affected = 0
        data_list = None

        if read_linenum:

            data_list, rows_affected = self.tc.read_data(read_linenum=read_linenum,
                                                         begin_index=self.read_index, begin_id_index=self.read_id_index,
                                                         begin_id_index_name=self.begin_id_index_name, dict_query=dict_query, extra_sql=extra_sql)

        logging.info("Read Data from %s, ReadIndex: %d, ReadIdIndex: %s , ReadIdIndexName: %s, ReadLineNum: %s, RowsAffected: %s" % (self.tc.table_name,
                                                                                                                                     self.read_index,
                                                                                                                                     self.read_id_index,
                                                                                                                                     self.begin_id_index_name,
                                                                                                                                     read_linenum,
                                                                                                                                     rows_affected))

        # no more data
        if rows_affected == 0:
            table_data_finish = 1
            return None, 0

        if self.begin_id_index_name:

            id_value = None

            if not dict_query:

                id_column_index = self.get_index_by_field_name(self.begin_id_index_name)

                last_record = data_list[rows_affected - 1]

                id_value = last_record[id_column_index]

            else:

                id_value = data_list[rows_affected - 1][self.begin_id_index_name]

            self.read_id_index = id_value  # int(id_value) + 1

            self.read_index = 0

        else:

            self.read_index = self.read_index + self.read_linenum

        self.total_read_num = self.total_read_num + rows_affected

        return data_list, rows_affected
