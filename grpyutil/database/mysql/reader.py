import json
from grpyutil.database import mysql
from grpyutil.database.file import logging


class DataReader(object):
    def __init__(self, begin_index, begin_id_index, sql_path, finish_flag, end_read_num, begin_id_index_name, read_linenum=30000):
        self.begin_index = begin_index
        self.begin_id_index = begin_id_index
        self.begin_id_index_name = begin_id_index_name

        self.sql_path = sql_path
        self.finish_flag = finish_flag
        self.end_read_num = end_read_num
        self.type = 0

        self.dc = mysql.get_db_conn(sql_path)
        table_name = self.dc.sql_config["table"]
        self.tc = mysql.TableConn(self.dc, table_name)

        self.read_linenum = read_linenum

        # reading index
        self.read_id_index = self.begin_id_index
        self.read_index = self.begin_index

        self.cache_data = None

        #
        self.total_read_num = 0

        self.fields_index_dict = {}
        self.fields_list = self.tc.get_filter_fields()

        for i in range(len(self.fields_list)):
            field = self.fields_list[i]
            self.fields_index_dict[field] = i

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

    def log_dumps(self):
        return "%d %s %s %s %s %s\n" % (self.finish_flag,
                                        self.sql_path, self.begin_index, self.begin_id_index, self.end_read_num, self.type)

    def get_next_rows(self):
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
                                                         begin_id_index_name=self.begin_id_index_name)

        logging.info("Read Data from %s, ReadIndex: %d, ReadIdIndex: %d , ReadIdIndexName: %s,ReadLineNum: %d, RowsAffected: %d" % (self.tc.table_name,
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

            id_column_index = self.get_index_by_field_name(self.begin_id_index_name)

            last_record = data_list[rows_affected - 1]

            # print(column_list, self.begin_id_name, id_column_index, last_record)

            id_value = last_record[id_column_index]

            self.read_id_index = int(id_value) + 1

            self.read_index = 0

        else:

            self.read_index = self.read_index + self.read_linenum

        self.total_read_num = self.total_read_num + rows_affected

        return data_list, rows_affected