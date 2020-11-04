#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : update_domain_code.py
# @Author: Cedar
# @Date  : 2020/5/22
# @Desc  :


import re  # 正则表达式库
import pymysql
import hashlib


def query_mysql(config_params, query_sql):
    """
    执行SQL
    :param config_params:
    :param query_sql:
    :return:
    """
    # 连接mysql
    config = {
        'host': config_params["host"],
        'port': config_params["port"],
        'user': config_params["user"],
        'passwd': config_params["passwd"],
        'db': config_params["db"],
        'charset': 'utf8mb4',
        'cursorclass': pymysql.cursors.DictCursor
    }
    results = None
    try:
        conn = pymysql.connect(**config)
        conn.autocommit(1)
        # 使用cursor()方法获取操作游标
        cur = conn.cursor()
        cur.execute(query_sql)  # 执行sql语句
        results = cur.fetchall()  # 获取查询的所有记录
        conn.close()  # 关闭连接
    except Exception as e:
        pass

    return results


def update_domain_code():
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
    select_sql = "select Domain_Code,Website_No from column_link where website_no like'S%' and Column_Extraction_Deep=0 GROUP BY Website_No;"

    # select_sql = "select Domain_Code,Website_No from column_link where website_no='GUOWAI' and Column_Extraction_Deep=0 GROUP BY Domain_Code;"
    update_sql = "update column_link set Website_No='{}' where Domain_Code='{}';"
    results = query_mysql(config, select_sql)  # 获取查询的所有记录
    # 遍历结果
    for row in results:
        domain_code = row['Domain_Code']
        website_no = row['Website_No']
        sql = update_sql.format(website_no, domain_code)
        print(sql)
        query_mysql(config, sql)


if __name__ == '__main__':
    update_domain_code()