#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : mysql_to_es.py
# @Author: Cedar
# @Date  : 2020/4/17
# @Desc  :

import time
import re
import json
import datetime
import hashlib
import pymysql
from elasticsearch import Elasticsearch
from elasticsearch import helpers


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


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


def run():
    # 查询待采集目标
    results = None
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    select_column = f"select ListPage_URL,ListPage_Title from listpage_url where Website_No IN (select Website_No from website where Website_Name like'%一点号%') and ListPage_URL like'http://www.yidianzixun.com/channel/m%';"
    try:
        results = query_mysql(extractor_116, select_column)
    except Exception as e:
        pass

    actions = []
    es = Elasticsearch("192.168.2.56:9200")
    for result in results:
        url = result["ListPage_URL"]
        source_name = result["ListPage_Title"]
        _id = get_token(url)
        threeDayAgo = (datetime.datetime.now() - datetime.timedelta(days=200))
        day_ago = threeDayAgo.strftime("%Y-%m-%dT%H:%M:%S.000+0800")
        print(source_name)

        # data = {
        #     "@timestamp": day_ago,
        #     "title": source_name,
        #     "listpage_url": url
        # }
        # es.index(index="yidianhao_all", doc_type="yidianhao", id=_id, body=data)
        action = {
            "_index": "yidianhao_all",
            "_type": "yidianhao",
            "_id": _id,
            "_source": {
                "title": source_name,
                "listpage_url": url,
                "@timestamp": day_ago,
            }
        }
        # 形成一个长度与查询结果数量相等的列表
        actions.append(action)

    helpers.bulk(es, actions)



if __name__ == '__main__':
    run()




