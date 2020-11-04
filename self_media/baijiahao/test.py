#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : test.py
# @Author: Cedar
# @Date  : 2020/7/1
# @Desc  :

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : es_to_es.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import datetime
from elasticsearch import Elasticsearch
import hashlib
from elasticsearch import helpers


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


def run():
    es = Elasticsearch(hosts="http://192.168.2.56:9200/")

    body = {
        "query": {
            "range": {
                "@timestamp": {
                    "gt": "{}T00:00:00".format("2020-07-01"),
                    "lt": "{}T23:59:59".format("2020-12-31"),
                    "time_zone": "Asia/Shanghai"
                }
            }
        }
    }
    query = es.search(index="baijiahao_all", doc_type="baijiahao",
                      body=body, size=100, scroll='5m')

    results = query['hits']['hits']  # es查询出的结果第一页
    total = query['hits']['total']  # es查询出的结果总量
    scroll_id = query['_scroll_id']  # 游标用于输出es查询出的所有结果

    items = []
    for item in results:
        items.append(item['_source'])

    for i in range(0, int(total / 100) + 1):
        # scroll参数必须指定否则会报错
        query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
        for item in query_scroll:
            items.append(item['_source'])

    actions = []
    es = Elasticsearch("192.168.2.56:9200")
    for i in items:
        url = i["listpage_url"]
        source_name = i["title"]
        _id = get_token(url)
        time_now = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800")
        # print(source_name)

        action = {
            "_index": "baijiahao_all",
            "_type": "baijiahao",
            "_op_type": "create",   # 如果存在会报错不会插入
            "_id": _id,
            "_source": {
                "title": source_name,
                "listpage_url": url,
                "@timestamp": time_now,
            }
        }
        # 形成一个长度与查询结果数量相等的列表
        actions.append(action)

    print(len(actions))
    # helpers.bulk(es, actions, raise_on_exception=False, raise_on_error=False)


if __name__ == '__main__':
    run()

