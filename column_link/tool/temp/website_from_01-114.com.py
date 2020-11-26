#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : website_from_01-114.com.py
# @Author: Cedar
# @Date  : 2020/11/17
# @Desc  :

import requests
import pymysql
from lxml import etree
import sys
sys.path.append('../..')
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
        print(e)

    return results


def parse_html_to_database(database_config, url, column_extraction_deep, domain_code_source, website_no, Is_Need_VPN, text):
    try:
        root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
        column_extraction_deep = 0
        items = root.xpath('//a')
        column_list = []
        for num, item in enumerate(items):
            title = "".join(item.xpath('.//text()'))
            listpage_url = "".join(item.xpath('./@href'))
            if (len(title) > 0) and ('go.php?' in listpage_url):
                listpage_url = listpage_url.replace('/go.php?', '')
                listpage_url = listpage_url.replace('http://www.01-114.com', '')
                print(title, listpage_url)
                # 去掉标点符号
                # 计算url MD5 先去掉http和末尾斜杆
                md5_source = listpage_url
                md5_source = md5_source.replace('http://', '')
                md5_source = md5_source.replace('https://', '')
                md5_source = md5_source.rstrip('/')
                record_md5_id = common.get_token(md5_source)
                domain_code = common.get_domain_code(listpage_url)
                host_code = common.get_host_code(listpage_url)

                host_code_index = listpage_url.index(host_code) + len(host_code)
                listpage_url = listpage_url[0:host_code_index] + '/'

                # 垃圾词、垃圾域名过滤
                level_score = 100
                score_detail = '{"status": True, "message": "root page"}'

                if level_score > 20:
                    column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', {level_score}, '{score_detail}')"
                    column_list.append(column)

        # 批量插入
        print(len(column_list))
        values = ",".join(column_list)
        insert_column = f"replace into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, score_detail) values{values};"
        # print(insert_column)
        query_mysql(database_config, insert_column)
        return True
    except Exception as e:
        pass



database_config = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}
url = 'http://01-114.com/'
response = requests.get(url)
response.encoding = 'utf-8'
text = response.text
parse_html_to_database(database_config, url, 0, '', '01-114', 0, text)

