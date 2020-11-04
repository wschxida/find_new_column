#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : insert-into-listpage_url-tools.py
# @Author: TLF
# @Date  : 2020/4/23
# @Desc  :将相关数据读取批量导入到mysql数据库

import hashlib
import pymysql
from elasticsearch import Elasticsearch
from lib import common


def input_from_es():
    """
    读取ES的数据，然后导入到mysql库中
    :return:
    """
    es = Elasticsearch(hosts="http://192.168.2.56:9200/")
    # body = {
    #     "query": {
    #         "match_all": {}
    #     }
    # }
    body = {
        "query": {
            "range": {
                "@timestamp": {
                    "gt": "{}T00:00:00".format("2020-01-01"),
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

    ls = []
    ls2 = []
    # for item in results: #循环读取ES第一页数据 每页为100条
    #     ls.append(item['_source'])
    #     print(ls)
    # insert_mysql(ls)
    for i in range(0, int(total / 100) + 1):  # 循环读取第2页及之后的数据
        # scroll参数必须指定否则会报错
        query_scroll = es.scroll(scroll_id=scroll_id, scroll='5m')['hits']['hits']
        for item in query_scroll:
            ls2.append(item['_source'])
            # print(ls2)
    insert_into_center_database(ls2)
    return


def input_from_file():
    file_add = "E:\\listpage_url.txt"
    file_ls = []
    with open(file_add, 'r', encoding='UTF-8') as f:
        lines = f.readlines()
        for line in lines:
            ls1 = {'time': '', 'title': '', 'url': ''}
            data = line.strip()
            data = data.replace('=http', ',http')
            # print(data)
            title = data.split(',')[0]
            url = data.split(',')[1]
            ls1['title'] = title
            ls1['url'] = url
            file_ls.append(ls1)
            # print(file_ls)
        insert_into_center_database(file_ls)


def input_from_mysql_150():
    connect = pymysql.connect(host='192.168.1.150', user='mmt_app', passwd='poms@db', db='test', charset='utf8mb4')
    cursor = connect.cursor()
    select_sql = "select name,url from facebook_author limit 200000;"
    sql_ls = []
    try:
        cursor.execute(select_sql)
        results = cursor.fetchall()
        for row in results:
            ls2 = {}
            ls2['title'] = row[0]
            ls2['url'] = row[1]
            sql_ls.append(ls2)
        # print(sql_ls)
        insert_into_center_database(sql_ls)
    except Exception as e:
        print("查询出错：%s" % e)
    finally:
        cursor.close()
        connect.close()


def input_from_column_link():
    extractor_118 = {'host': '192.168.1.133', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    select_column_pattern = "select URL as ListPage_URL,Title as ListPage_Title,Domain_Code,Host_Code,Level_Score as Last_Check_Score_Text " \
                            "from column_link where Website_No='GUOWAI' ORDER BY Host_Code limit {},{};"

    for i in range(130):
        # print(i*100, 100)
        select_column = select_column_pattern.format(i*1000, 1000)
        print(select_column)
        try:
            results = common.query_mysql(extractor_118, select_column)
            # print(results)
            insert_into_center_database(results)

        except Exception as e:
            print(e)


def insert_into_center_database(input_data_list):
    """
    查询相关数据库，解析相关数据并批量insert到mysql对应表中
    :param input_data_list: 类型list，每个item为字典，key全部要以listpage_url表字段命名
    [{"ListPage_Title":"1234","ListPage_URL":"https://v2.sohu.com/1",...},{"ListPage_Title":"2345","ListPage_URL":"https://weibo.com/p/aj/v6/mblog/",...}]
    :return:
    """
    center_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}

    column_list = []
    for i in input_data_list:

        website_no = 'S18605'
        listpage_url = i['ListPage_URL']
        # listpage_url_dm5 = ''
        listpage_title = i['ListPage_Title']
        domain_code = i['Domain_Code']
        host_code = i['Host_Code']
        last_check_score_text = i['Last_Check_Score_Text']
        listpage_save_rule = 3
        is_enabled = 1
        linkurl_min_length = 10
        linktext_min_length = 4

        # 计算url MD5 先去掉http和末尾斜杆
        md5_source = listpage_url
        md5_source = md5_source.replace('http://', '')
        md5_source = md5_source.replace('https://', '')
        md5_source = md5_source.rstrip('/')
        md5_source = md5_source.lower()
        listpage_url_dm5 = common.get_token(md5_source)

        column = f"('{website_no}', '{listpage_url}', '{listpage_url_dm5}','{listpage_title}','{domain_code}','{host_code}',{last_check_score_text},{listpage_save_rule},{is_enabled},{linkurl_min_length},{linktext_min_length})"
        column_list.append(column)

    values = ",".join(column_list)
    insert_column = f"insert ignore into listpage_url(Website_No,ListPage_URL,ListPage_URL_MD5,ListPage_Title,Domain_Code,Host_Code,Last_Check_Score_Text,ListPage_Save_Rule,Is_Enabled,LinkURL_Min_Length,LinkText_Min_Length) values{values};"
    # print(insert_column)
    try:
        common.query_mysql(center_116, insert_column)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    # input_es()
    # insert_mysql(ls)
    # input_file()
    input_from_column_link()
