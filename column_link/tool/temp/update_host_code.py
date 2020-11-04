#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : update_host_code.py
# @Author: Cedar
# @Date  : 2020/5/22
# @Desc  :


import time
import json
import lib.common as common
import pymysql


def update_host_code():
    # 连接mysql
    config = {
        'host': '192.168.1.118',
        'port': 3306,
        'user': 'root',
        'passwd': 'poms@db',
        'db': 'mymonitor',
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }

    # 1.查询操作
    # 编写sql 查询语句
    select_sql = "select Column_Link_ID,URL from column_link where host_code is null limit 1000;"
    # update_sql = "update column_link set host_code='{}' where Column_Link_ID={};"
    update_sql_pattern = "UPDATE column_link SET host_code = CASE Column_Link_ID {} END WHERE Column_Link_ID IN {};"
    when_then_pattern = " WHEN {} THEN '{}' "
    id_list = []
    try:
        results = common.query_mysql(config, select_sql)  # 获取查询的所有记录

        when_then = ""
        # 遍历结果
        for row in results:
            Column_Link_ID = row['Column_Link_ID']
            url = row['URL']
            host_code = common.get_host_code(url)
            if len(host_code) > 50:
                continue
            # sql = update_sql.format(host_code, Column_Link_ID)
            when_then = when_then + when_then_pattern.format(Column_Link_ID, host_code)
            id_list.append(Column_Link_ID)

        id_tuple = tuple(id_list)
        sql = update_sql_pattern.format(when_then, id_tuple)
        print(sql)
        try:
            common.query_mysql(config, sql)
        except Exception as e:
            print(e)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(31481):
        update_host_code()
        # time.sleep(1)
