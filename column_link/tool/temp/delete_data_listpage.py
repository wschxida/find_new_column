#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_listpage_to_listpage.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import pymysql


# import lib.common as common
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
        print(e)

    return results


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = f"""
                    select ListPage_URL_ID from listpage_url l where 
                    EXISTS (select ListPage_URL_ID from cloud_listpage_url_backup20200911 c where l.ListPage_URL_ID=c.ListPage_URL_ID)
                    and l.ListPage_URL_ID BETWEEN {start} AND {end};
                    """

    try:
        results = query_mysql(extractor_116, select_column)
        print('116 select count: ', len(results))
        column_list = []
        for item in results:
            ListPage_URL_ID = item['ListPage_URL_ID']
            column_list.append(ListPage_URL_ID)

        # 批量插入
        # values = ",".join(column_list)
        # 只有一条时，由于in (11，) 里面有逗号会报错，所以加上一个值
        if len(column_list) == 1:
            column_list.append(1)
            print(column_list)
        values = tuple(column_list)
        insert_column = f"""delete from listpage_url where ListPage_URL_ID in {values}"""
        # print(insert_column)
        if len(column_list) > 0:
            query_mysql(extractor_116, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(0, 116):
        print(i*1000000, (i+1)*1000000-1)
        main(i*1000000, (i+1)*1000000-1)
