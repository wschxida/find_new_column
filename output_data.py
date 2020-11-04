#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_es_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib


es = Elasticsearch(hosts="http://192.168.2.56:9200/")

body = {
    "query": {
        "match_all": {}
    }
}
query = es.search(index="qiluyidianhao", doc_type="qiluyidianhao",
                  body=body, size=100, scroll='5m')

results = query['hits']['hits']  # es查询出的结果第一页
total = query['hits']['total']  # es查询出的结果总量
scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果

for item in results:
    title = item['_source']['title']
    if len(item['_source']['title']) < 1:
        title = 'None'
    print(title + '=' + item['_source']['listpage_url'])

for i in range(0, int(total/100)+1):
    # scroll参数必须指定否则会报错
    query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
    for item in query_scroll:
        title = item['_source']['title']
        if len(item['_source']['title']) < 1:
            title = 'None'
        print(title + '=' + item['_source']['listpage_url'])


