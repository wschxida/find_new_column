#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_file_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


from datetime import datetime
import hashlib
import pymysql
import lib.common as common
import requests
from bs4 import BeautifulSoup


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


def get_encoding(res):
    encoding = 'utf-8'
    if res.encoding == 'ISO-8859-1':
        encodings = requests.utils.get_encodings_from_content(res.text)
        if encodings:
            encoding = encodings[0]
        else:
            encoding = res.apparent_encoding
    return encoding


def main():
    fn = open('listpage.txt', 'r', encoding='UTF-8')  # 打开文件
    column_list = []
    for i in fn:
        try:
            title = i.split('=')[0].strip()
            listpage_url = i.split('=')[1].strip()

            domain_code = common.get_domain_code(listpage_url)
            host_code = common.get_host_code(listpage_url)
            host_code_index = listpage_url.index(host_code)+len(host_code)
            listpage_url = listpage_url[0:host_code_index] + '/'
            # 计算url MD5 先去掉http和末尾斜杆
            md5_source = listpage_url
            md5_source = md5_source.replace('http://', '')
            md5_source = md5_source.replace('https://', '')
            md5_source = md5_source.rstrip('/')
            record_md5_id = common.get_token(md5_source)

            if len(title) < 1:
                try:
                    response = requests.get(listpage_url, timeout=5)
                    # print(response.status_code)
                    if response.status_code == 200:
                        encoding = get_encoding(response)
                        # print(encoding)
                        response.encoding = encoding
                        soup = BeautifulSoup(response.text, 'lxml')
                        # print(soup.title.text)
                        title = soup.title.text
                    else:
                        continue
                except:
                    pass
            try:
                title = title.split('－')[0].strip()
            except Exception as e:
                title = title
            try:
                title = title.split('_')[0].strip()
            except Exception as e:
                title = title
            try:
                title = title.split('-')[0].strip()
            except Exception as e:
                title = title

            level_score, Score_Detail = common.is_need_filter(title, listpage_url, True)
            # print(level_score, Score_Detail, title, listpage_url)
            if level_score > -100:
                level_score = '100'
                Score_Detail = '"{\"status\": True, \"message\": \"root page\"}"'

                website_no = 'AD_SELECTED'
                column_extraction_deep = 0
                column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{Score_Detail}')"
                # column_list.append(column)
                print(column)
                # 批量插入
                extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db',
                                 'db': 'mymonitor'}
                values = column
                insert_column = f"insert ignore into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, Score_Detail) values{values};"
                # print(insert_column)
                try:
                    common.query_mysql(extractor_118, insert_column)
                except Exception as e:
                    pass

        except Exception as e:
            pass

    fn.close()  # 关闭文件


if __name__ == '__main__':
    main()
