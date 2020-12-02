#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 1_input_data_column_to_cloud.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import sys
sys.path.append('../..')
import lib.common as common
import random
import re


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 每次读取100万，并按domain_code排序，这样分配节点时便于把域名相同的分在一个组
    select_column = f"select title,url,domain_code,record_md5_id from column_link where Column_Link_ID BETWEEN {start} AND {end};"

    try:
        results = common.query_mysql(extractor_118, select_column)
        column_list = []
        count = 0
        print('len(results): ', len(results))
        for item in results:
            Cloud_Server_ID = random.randint(10000, 10062)
            Listpage_URL = item['url']
            # 过滤空格换行等
            p = re.compile('\s+')
            Listpage_URL = re.sub(p, '', Listpage_URL)
            ListPage_Title = item['title']
            Domain_Code = item['domain_code']
            Record_MD5_ID = item['record_md5_id']

            column = f"({Cloud_Server_ID}, '{Listpage_URL}', '{ListPage_Title}', '{Domain_Code}', '{Record_MD5_ID}')"
            column_list.append(column)

            # 每1000条插入一次，分批
            if count > 0 and not count % 1000:
                print(count)
                values = ",".join(column_list)
                insert_column = f"insert ignore into cloud_listpage_url(Cloud_Server_ID, Listpage_URL, ListPage_Title, Domain_Code, Record_MD5_ID) values{values};"
                # print(insert_column)
                common.query_mysql(extractor_116, insert_column)
                column_list = []

            count += 1

        print('len(column_list): ', len(column_list))
        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into cloud_listpage_url(Cloud_Server_ID, Listpage_URL, ListPage_Title, Domain_Code, Record_MD5_ID) values{values};"
        # print(insert_column)
        common.query_mysql(extractor_116, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(0, 861):
        print(i*1000000, (i+1)*1000000-1)
        main(i*1000000, (i+1)*1000000-1)
