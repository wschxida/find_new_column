#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_es_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib
import pymysql
import lib.common as common


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


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


es = Elasticsearch(hosts="http://192.168.2.56:9200/")

body = {
    "query": {
        "match_all": {}
    }
}
query = es.search(index="chinaz_zonghe", doc_type="chinaz_zonghe",
                  body=body, size=100, scroll='5m')

results = query['hits']['hits']  # es查询出的结果第一页
total = query['hits']['total']  # es查询出的结果总量
scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果


column_list = []
for item in results:
    title = item['_source']['title']
    listpage_url = item['_source']['listpage_url']
    # 计算url MD5 先去掉http和末尾斜杆
    md5_source = listpage_url
    md5_source = md5_source.replace('http://', '')
    md5_source = md5_source.replace('https://', '')
    md5_source = md5_source.rstrip('/')
    record_md5_id = common.get_token(md5_source)
    if len(item['_source']['title']) < 1:
        title = 'None'
    title = title.strip()
    listpage_url = listpage_url.strip()
    website_no = 'ZONGHE'
    column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}')"
    column_list.append(column)


for i in range(0, int(total/100)+1):
    # scroll参数必须指定否则会报错
    query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
    for item in query_scroll:
        title = item['_source']['title']
        listpage_url = item['_source']['listpage_url']
        # 计算url MD5 先去掉http和末尾斜杆
        md5_source = listpage_url
        md5_source = md5_source.replace('http://', '')
        md5_source = md5_source.replace('https://', '')
        md5_source = md5_source.rstrip('/')
        record_md5_id = common.get_token(md5_source)
        if len(item['_source']['title']) < 1:
            title = 'None'
        title = title.strip()
        listpage_url = listpage_url.strip()
        website_no = 'ZONGHE'
        column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}')"
        column_list.append(column)


extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db',
                                     'db': 'mymonitor'}
values = ",".join(column_list)
insert_column = f"insert ignore into column_link(Title, URL, record_md5_id, website_no) values{values};"
# print(insert_column)
query_mysql(extractor_118, insert_column)
