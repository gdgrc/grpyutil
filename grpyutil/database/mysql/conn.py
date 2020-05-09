from .parse import *

import pymysql


def get_table_conn(sql_path):
    sql_config = parse_mysql_path(sql_path)
    if not sql_config:
        msg = "mysql_read fail!!  data path not exists,filename: %s" % sql_path
        raise Exception(msg)
    """
    conn = pymysql.Connect(host=sql_config["host"], port=sql_config["port"],
                           user=sql_config['user'], password=sql_config["password"],
                           database=sql_config["database"],
                           charset=sql_config["coding"] if "coding" in sql_config else "utf8mb4")
    """
    db_conn = get_db_conn(sql_path)

    return TableConn(db_conn, sql_config["table"])


def get_db_conn(sql_path):

    conn = DbConn(sql_path)

    return conn


class DbConn(object):
    def __init__(self, sql_path):
        sql_config = parse_mysql_path(sql_path)
        if not sql_config:
            msg = "mysql_read fail!!  data path not exists,filename: %s" % sql_path
            raise Exception(msg)

        conn = pymysql.Connect(host=sql_config["host"], port=sql_config["port"],
                               user=sql_config['user'], password=sql_config["password"],
                               database=sql_config["database"],
                               charset=sql_config["coding"] if "coding" in sql_config else "utf8mb4",
                               autocommit=False)

        self.db_conn = conn
        self.sql_path = sql_path
        self.sql_config = sql_config

    def check_and_fix(self):
        self.db_conn.ping(reconnect=True)

    def mogrify(self, query, args=None):
        cursor = self.db_conn.cursor()
        return cursor.mogrify(query, args)

    def execute(self, query, args=None):
        cursor = self.db_conn.cursor()
        ret = cursor.execute(query, args)

        cursor.close()

        return ret

    def executemany(self, query, args):
        cursor = self.db_conn.cursor()

        ret = cursor.executemany(query, args)

        cursor.close()

        return ret

    def dict_query(self, query, args=None):

        cursor = self.db_conn.cursor(cursor=pymysql.cursors.DictCursor)
        rows_length = cursor.execute(query, args)
        rows = cursor.fetchall()

        cursor.close()

        return rows, rows_length

    def query(self, query, args=None):

        cursor = self.db_conn.cursor()
        rows_length = cursor.execute(query, args)
        rows = cursor.fetchall()

        cursor.close()

        return rows, rows_length

    def commit(self):
        cursor = self.db_conn.cursor()

        ret = cursor.execute("commit")

        cursor.close()
        return ret

    def query_columns(self, query, args=None):
        cursor = self.db_conn.cursor()
        rows_length = cursor.execute(query, args)
        description = cursor.description

        columns = []

        for d in description:
            columns.append(d[0])

        cursor.close()

        return columns

    def close(self):
        return self.db_conn.close()

    def travel_tables(self, column_groups=None):
        """
        usage: self.travel_tables([['cell','name'],['uid','cell'],['uid']])
        """
        sql = "SHOW TABLES"

        rows, rows_length = self.query(sql)

        total_tables = []

        for row in rows:
            table_name = row[0]
            good_flag = True

            if column_groups is not None:
                good_flag = False

                tc = TableConn(self, table_name)

                fields_list = tc.read_fields()

                for column_group_list in column_groups:
                    good_flag = True
                    for column_name in column_group_list:
                        if column_name not in fields_list:
                            good_flag = False
                            break

            """
            tmp_sql = "DESC `%s`" % table_name
            tmp_rows_length = cursor.execute(tmp_sql)

            tmp_rows = cursor.fetchall()
           
            tmp_list = []
            for tmp_row in tmp_rows:
                tmp_list.append(row['Field'])
            """

            if good_flag:

                total_tables.append(table_name)

        return total_tables


class TableConn(object):
    def __init__(self, db_conn_object, table_name):
        if not db_conn_object or not table_name:
            msg = "mysql_read_data fail!!  db_conn_object and table must exist both! table: %s" % (table_name)
            raise Exception(msg)

        self.db_conn_object = db_conn_object
        self.table_name = table_name

        self._fields_list = None

        self._detail_fields_list = None

        self._origin_fields_list = None

        self._constraint_fields_list = None

        self._index_fields_dict = None

        self.read_fields()

    def copy(self, table_name):

        return TableConn(self.db_conn_object, table_name)

    def get_table_name(self):
        return self.table_name

    def get_sql_path(self):
        return "sql://%s:%d/%s:%s/%s/%s" % (self.db_conn_object.sql_config["host"],
                                            self.db_conn_object.sql_config["port"],
                                            self.db_conn_object.sql_config["user"],
                                            self.db_conn_object.sql_config["password"],
                                            self.db_conn_object.sql_config["database"],
                                            self.table_name,

                                            )

    def get_unique_name(self):

        return "%s_%s_%s" % (self.db_conn_object.sql_config["host_ipv4"].get_ip4(), self.db_conn_object.sql_config["database"], self.table_name)

    def read_fields(self):

        if self._fields_list is not None:
            return self._fields_list

        sql = "DESC `%s`" % self.table_name

        rows, rows_length = self.db_conn_object.query(sql)

        tmp_list = []
        for row in rows:

            tmp_list.append(row[0].lower())

        self._fields_list = tmp_list

        self._filter_fields = self._fields_list

        return self._fields_list

    def read_origin_fields(self):
        if self._origin_fields_list is not None:
            return self._origin_fields_list

        sql = "DESC `%s`" % self.table_name

        rows, rows_length = self.db_conn_object.query(sql)

        tmp_list = []
        for row in rows:

            tmp_list.append(row[0])

        self._origin_fields_list = tmp_list

        return self._origin_fields_list

    def read_constraint_fields(self):
        sql = "select * from information_schema.key_column_usage where table_schema =%s  and table_name = %s "
        rows, rows_length = self.db_conn_object.query(sql, (self.db_conn_object.sql_config["database"], self.table_name))

        self._constraint_fields_list = []
        self._constraint_fields_map = {}

        for row in rows:

            name = row[6]
            referenced_table_database = row[9]
            referenced_table_name = row[10]
            referenced_table_key = row[11]

            self._constraint_fields_list.append({"referenced_table_database": referenced_table_database,
                                                 "name": name, "referenced_table_name": referenced_table_name, "referenced_table_key": referenced_table_key})

            self._constraint_fields_map[name] = {"referenced_table_database": referenced_table_database,
                                                 "name": name, "referenced_table_name": referenced_table_name, "referenced_table_key": referenced_table_key}

        return self._constraint_fields_list

    def read_detail_fields(self):
        if self._detail_fields_list is not None:
            return self._detail_fields_list

        sql = "select * from information_schema.columns where table_schema =%s  and table_name = %s"

        rows, rows_length = self.db_conn_object.query(sql, (self.db_conn_object.sql_config["database"], self.table_name))

        self._detail_fields_list = []

        self._detail_fields_map = {}

        self.read_constraint_fields()
        for row in rows:
            comment = row[19]
            name = row[3]

            data_type = row[7]

            char_index = data_type.find("(")
            if char_index >= 0:
                name = name[0:char_index]

            data_dict = {"comment": comment, "name": name, "data_type": data_type}

            if name in self._constraint_fields_map:
                data_dict.update(self._constraint_fields_map[name])

            self._detail_fields_list.append(data_dict)

            self._detail_fields_map[name] = data_dict

        return self._detail_fields_list

    def read_index_fields(self):
        if self._index_fields_dict is not None:
            return self._index_fields_dict

        self._index_fields_dict = {}

        sql = " show index from %s " % self.table_name

        rows, rows_length = self.db_conn_object.query(sql)
        """
            Table: railway
           Non_unique: 1
             Key_name: uid
         Seq_in_index: 1
          Column_name: uid
            Collation: A
          Cardinality: 144530576
             Sub_part: NULL
               Packed: NULL
                 Null: 
           Index_type: BTREE
              Comment: 
        Index_comment: 

        ('railway05_00306_711_func_data_md5decode', 1, 'uid', 1, 'uid', 'A', None, None, None, '', 'BTREE', '', '')


        """
        for row in rows:

            if row[1] not in self._index_fields_dict:
                self._index_fields_dict[row[2]] = {}

            self._index_fields_dict[row[2]][row[3]] = {"table": row[0], "non_unique": row[1],
                                                       "key_name": row[2], "seq_in_index": row[3], "column_name": row[4],
                                                       "collation": row[5], "cardinality": row[6], "sub_part": row[7],
                                                       "packed": row[8], "null": row[9], "index_type": row[10],
                                                       "comment": row[11], "index_comment": row[12]}

        return self._index_fields_dict

    def check_if_column_is_index(self, column):
        index_fields_dict = self.read_index_fields()

        for index_name, seq_dict in self._index_fields_dict.items():

            if seq_dict[1]["column_name"] == column:
                return True

        return False

    def commit(self):

        return self.db_conn_object.commit()

    def read_table_comment(self):

        table_definition = self.read_table_definition()

        comment = ""

        key_word = "COMMENT="
        if key_word in table_definition:
            comment = table_definition.split(key_word, 1)[1].split("'", 2)[1]

        return comment

    def read_table_definition(self):
        """
        get upper table definition
        """

        sql = "SHOW CREATE TABLE `%s`" % self.table_name

        rows, rows_length = self.db_conn_object.query(sql)

        return (rows[0][1]).upper()

    def set_filter_fields(self, target_fields, force=False):

        filter_fields = []

        for item in target_fields:
            if item in self._fields_list or force:
                filter_fields.append(item)

        self._filter_fields = filter_fields

        return True

    def get_filter_fields(self):
        return self._filter_fields

    def dict_query(self, query, args=None):

        return self.db_conn_object.dict_query(query, args)

    def query(self, query, args=None):

        return self.db_conn_object.query(query, args)

    def execute(self, query, args=None):

        return self.db_conn_object.execute(query, args)

    def executemany(self, query, args=None):

        return self.db_conn_object.executemany(query, args)

    def read_data_columns(self):

        self.db_conn_object.check_and_fix()

        if not self._filter_fields:
            raise Exception("no filter fields")

        # print(self._filter_fields)

        sql = ""
        try:
            sql = "SELECT %s FROM `%s` " % (','.join("%s" % item for item in self._filter_fields), self.table_name)
            args_list = []

            sql += " LIMIT 1"

            return self.db_conn_object.query_columns(sql, args_list)
        except Exception as e:
            raise Exception("sql execute error: %s, sql: %s" % (e, sql))

    def read_data(self, read_linenum=0, begin_index=0, begin_id_index=None, begin_id_index_name="", dict_query=False, extra_sql=""):

        self.db_conn_object.check_and_fix()

        if not self._filter_fields:
            raise Exception("no filter fields")

        sql = ""
        try:
            sql = "SELECT %s FROM `%s` " % (','.join("%s" % item for item in self._filter_fields), self.table_name)
            args_list = []

            # id begin may be faster
            if begin_id_index_name and begin_id_index:
                sql += " WHERE `%s`> " % (begin_id_index_name)
                sql += " %s "

                args_list.append(begin_id_index)

            if extra_sql:
                if "WHERE" in sql:
                    sql += (" AND " + extra_sql)
                else:
                    sql += extra_sql

            # limit begin
            if read_linenum > 0:
                sql += " LIMIT "
                if begin_index:
                    begin = begin_index if begin_index > -1 else 0
                    sql += " %d, " % (begin)

                sql += " %d" % (read_linenum)

            # print(sql, args_list)
            rows = None
            rows_length = None
            if dict_query:
                rows, rows_length = self.db_conn_object.dict_query(sql, args_list)
            else:
                rows, rows_length = self.db_conn_object.query(sql, args_list)

            """
            # i do not think we should travel
            for row in rows:
                tmp_list = []
                for data in row:
                    data_tmp = ''
                    if data is not None:
                        data_tmp = str(data)

                    tmp_list.append(data)
                data_list.append(tmp_list)
            """

            # msg = "Finish reading table %s,read_index: %d,total_len: %d,one for fields' name" % (tbname, demoindex, len(data_list))
            # echomsg(msg, False)

            return rows, rows_length
        except Exception as e:
            raise Exception("sql execute error: " + sql)
