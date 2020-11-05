#! /usr/bin/env python
# -*- coding: utf-8 -*-
# vim:fenc=utf-8


import traceback
from datetime import datetime
import argparse
from grpyutil.database import mysql
import time
import sys
import logging
sys.path.insert(0, '../')
sys.path.insert(0, '.')
import os

from grpyutil.database.file import logging
import copy
# COLUMN_NAME_IDENTITY = "uid"
# log_path = os.path.join(file_log_path, "%s.log" % (tc.get_unique_name()))

logging.basicConfig(
    filename="../log/tool_mysql_table_to_django_model.log",
    format='%(asctime)s - %(levelname)s: %(message)s',
    level=logging.DEBUG
)


def main(args):

    dr = mysql.DataReader(0, None, args.f, 0, 0, "", read_linenum=30)
    # {'comment': '广告投放的城市', 'name': 'ad_city', 'data_type': 'varchar', 'length': 50}

    table_comment = dr.tc.read_table_comment()
    detail_fields = dr.tc.read_detail_fields()
    generate_string = ""
    print(detail_fields)
    table_name = dr.tc.table_name

    table_name_arr = table_name.split('_')

    model_name = ""
    for item in table_name_arr:
        model_name += item[0].upper()
        if len(item) > 1:
            model_name += item[1:]

    generate_string += "class %s(models.Model):\n" % (model_name)

    if not table_comment:
        raise Exception("table: %s 's table_comment should not be empty" % (table_name))

    generate_string += "\tclass Meta:\n"
    generate_string += "\t\tdb_table = '%s'\n" % (table_name)
    generate_string += "\n\t# table_desc: %s\n" % (table_comment)

    model_field_list = []
    for item in detail_fields:

        model_field_list.append(item)
        field_type = ""
        if "char" in item["data_type"]:
            field_type = "CharField(max_length=%d)" % (item["length"] + 20)
        elif "datetime" == item["data_type"]:
            field_type = "DateTimeField()"
        elif "int" in item["data_type"]:
            field_type = "IntegerField()"
        else:
            raise Exception("unknow type: %s" % item["data_type"])

        comment = item["comment"]
        if not comment:
            raise Exception("comment should not be empty %s" % (item))
        generate_string += "\t%s = models.%s  # field_desc: %s\n" % (item["name"], field_type, comment)

    print(generate_string)


if __name__ == '__main__':

    today = datetime.now().strftime("%Y-%m-%d")
    parser = argparse.ArgumentParser(description="python mysql_table_to_django_model.py --f=sql://127.0.0.1:3306/dev:xx/wechatadmin/mmw_ad_orders")

    parser.add_argument(
        '--bdb_i',
        help="bdb begin table index",
        type=int,
        nargs="?",
        required=False,
        default=1
    )

    parser.add_argument(
        '--npw',
        help="write rows num per write",
        type=int,
        nargs="?",
        required=False,
        default=70000
    )

    parser.add_argument(
        '--npr',
        help="read rows num per read, can not lower than 10w",
        type=int,
        nargs="?",
        required=False,
        default=100000
    )

    parser.add_argument(
        '--dest',
        help="../src/",
        type=str,
        nargs="?",
        required=False,
        default="../src/"
    )

    parser.add_argument(
        '--bii',
        help="begin_id_index 231000",
        type=int,
        nargs="?",
        required=False,
        default=0
    )

    parser.add_argument(
        '--bi',
        help="begin_index 231000",
        type=int,
        nargs="?",
        required=False,
        default=0
    )
    parser.add_argument(
        '--f',
        help="sql://127.0.0.1:3306/root:paasword/func_effect_data/table_name",
        type=str,
        nargs="?",
        required=True
        # default=today
    )

    args = parser.parse_args()

    # logging.info("begin to run the script %s, get time: %s" % (__file__, args.time))

    # filterwarnings('ignore', category=MySQLdb.Warning)

    try:
        main(args)

    except Exception as e:
        """
        logging.clear()

        logging.basicConfig(
            filename=os.path.join(args.log, "%s.log" % __file__),
            format='%(asctime)s - %(levelname)s: %(message)s',
            level=logging.INFO
        )
        """

        exstr = traceback.format_exc()
        msg = "[%s][FATAL] day:%s stack:%s , err: %s" % (time.strftime("%Y-%m-%d %H:%M:%S"), today, exstr, e)
        sys.stderr.write(msg)
        logging.info(msg)

    # resetwarnings()
