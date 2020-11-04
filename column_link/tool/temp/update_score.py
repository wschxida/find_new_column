#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : update_score.py
# @Author: Cedar
# @Date  : 2020/5/22
# @Desc  :


import time
import json
import lib.common as common
import pymysql


def update_score():
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
    select_sql = "select Column_Link_ID,URL,Title from column_link where Level_Score is null limit 1000;"
    update_sql_pattern = "UPDATE column_link SET " \
                         "Level_Score = CASE Column_Link_ID {0} END, " \
                         "Score_Detail = CASE Column_Link_ID {1} END  " \
                         "WHERE Column_Link_ID IN {2};"
    when_then_score_pattern = " WHEN {} THEN {} "
    when_then_detail_pattern = " WHEN {} THEN '{}' "
    id_list = []
    try:
        results = common.query_mysql(config, select_sql)  # 获取查询的所有记录
        print(results)

        when_then_score = ""
        when_then_detail = ""
        # 遍历结果
        for row in results:
            Column_Link_ID = row['Column_Link_ID']
            url = row['URL']
            title = row['Title']
            level_score, status = common.is_need_filter(title, url, False)
            # print(level_score, status)
            when_then_score = when_then_score + when_then_score_pattern.format(Column_Link_ID, level_score)
            when_then_detail = when_then_detail + when_then_detail_pattern.format(Column_Link_ID, status)
            id_list.append(Column_Link_ID)

        id_tuple = tuple(id_list)
        sql = update_sql_pattern.format(when_then_score, when_then_detail, id_tuple)
        print(sql)
        try:
            common.query_mysql(config, sql)
        except Exception as e:
            print(e)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(100000):
        update_score()
        # time.sleep(1)
