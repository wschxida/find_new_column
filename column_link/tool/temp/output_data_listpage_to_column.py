#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_listpage_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common


def main(start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = f"select Website_No,ListPage_URL,ListPage_Title from listpage_url where ListPage_URL_ID IN " \
                    f"(select ListPage_URL_ID from cloud_listpage_url where cloud_listpage_url_id BETWEEN {start} AND {end});"

    try:
        results = common.query_mysql(extractor_116, select_column)
        column_list = []
        for item in results:
            URL = item['ListPage_URL']
            Title = item['ListPage_Title']
            Website_No = item['Website_No']
            Domain_Code = common.get_domain_code(URL)
            Host_Code = common.get_host_code(URL)
            # 计算url MD5 先去掉http和末尾斜杆
            md5_source = URL
            md5_source = md5_source.replace('http://', '')
            md5_source = md5_source.replace('https://', '')
            md5_source = md5_source.rstrip('/')
            Record_MD5_ID = common.get_token(md5_source)
            Level_Score, Score_Detail = common.is_need_filter(Title, URL)
            Column_Extraction_Deep = 1

            column = f"({Column_Extraction_Deep}, '{URL}', '{Title}', '{Domain_Code}', '{Host_Code}', '{Record_MD5_ID}', {Level_Score}, '{Score_Detail}', '{Website_No}')"
            if Level_Score > 20:
                column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into column_link(Column_Extraction_Deep, URL, Title, Domain_Code, Host_Code, Record_MD5_ID, Level_Score, Score_Detail, Website_No) values{values};"
        # print(insert_column)
        common.query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    # for i in range(2483, 5699):
    for i in range(5000, 5699):
        print(i*10000, (i+1)*10000-1)
        main(i*10000, (i+1)*10000-1)
