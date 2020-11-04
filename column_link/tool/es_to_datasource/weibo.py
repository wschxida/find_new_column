#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : weibo.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common
import warnings

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = f'''
        SELECT
            lu.listpage_url,
            lu.listpage_title,
            w.Website_Important_Level
        FROM
            listpage_url lu
        LEFT JOIN website w ON lu.website_no = w.website_no
        WHERE
            w.Website_Name like'%新浪微博%' 
            and w.Website_Name like'%作者列表%'
            and lu.ListPage_URL_ID BETWEEN {start} AND {end};
    '''

    try:
        results = common.query_mysql(extractor_116, select_column)
        column_list = []
        count = 0
        print(len(results))
        for item in results:
            author_name = item['listpage_title']
            # author_account = item['listpage_url'].replace('http://', '')
            author_url = item['listpage_url']
            try:
                author_id = author_url.split('&id=100505')[1].split('&feed_type')[0]
                author_type = 'user'
            except Exception as e:
                try:
                    author_id = author_url.split('weibo.com/u/')[1].replace('?is_all=1', '')
                    author_type = 'user'
                except Exception as e:
                    try:
                        author_id = author_url.split('weibo.com/p/')[1].replace('/wenzhang', '')
                        author_type = 'page'
                    except Exception as e:
                        author_id = author_name
                        author_type = ''

            is_added = 1
            column = f"('{author_name}', '{author_url}', '{author_id}', '{author_type}', {is_added})"
            column_list.append(column)

            # 每1万条插入一次，分批
            if count > 0 and not count % 10000:
                values = ",".join(column_list)
                insert_column = f"insert ignore into author_weibo(author_name, author_url, author_id, author_type, is_added) values{values};"
                # print(insert_column)
                common.query_mysql(extractor_118, insert_column)
                column_list = []
                print(count)

            count += 1

        values = ",".join(column_list)
        insert_column = f"insert ignore into author_weibo(author_name, author_url, author_id, author_type, is_added) values{values};"
        # print(insert_column)
        common.query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(0, 200):
        print(i*1000000, (i+1)*1000000-1)
        main(i*1000000, (i+1)*1000000-1)




