#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 1_input_data_column_to_cloud.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    # 每次读取100万，并按domain_code排序，这样分配节点时便于把域名相同的分在一个组
    select_column = f"select title,url,domain_code,record_md5_id,level_score from column_link where Column_Link_ID>33114420 and Column_Link_ID BETWEEN {start} AND {end} ORDER BY Domain_Code;"

    try:
        results = common.query_mysql(extractor_118, select_column)
        column_list = []
        count = 0
        print('len(results): ', len(results))
        for item in results:
            Listpage_URL = item['url']
            ListPage_Title = item['title']
            Domain_Code = item['domain_code']
            Record_MD5_ID = item['record_md5_id']
            Level_Score = item['level_score']

            column = f"('{Listpage_URL}', '{ListPage_Title}', '{Domain_Code}', '{Record_MD5_ID}', {Level_Score})"
            column_list.append(column)

            # 每1000条插入一次，分批
            if count > 0 and not count % 1000:
                print(count)
                values = ",".join(column_list)
                insert_column = f"insert ignore into cloud_listpage_url(Listpage_URL, ListPage_Title, Domain_Code, Record_MD5_ID, Level_Score) values{values};"
                # print(insert_column)
                common.query_mysql(extractor_116, insert_column)
                column_list = []

            count += 1

        print('len(column_list): ', len(column_list))
        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into cloud_listpage_url(Listpage_URL, ListPage_Title, Domain_Code, Record_MD5_ID, Level_Score) values{values};"
        # print(insert_column)
        common.query_mysql(extractor_116, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(33, 130):
        print(i*1000000, (i+1)*1000000-1)
        main(i*1000000, (i+1)*1000000-1)
