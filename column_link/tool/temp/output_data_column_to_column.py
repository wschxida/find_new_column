#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_column_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common


def main(start, end):
    extractor_133 = {'host': '192.168.1.133', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = f"select * from column_link_2 where Column_Link_ID BETWEEN {start} AND {end};"

    try:
        results = common.query_mysql(extractor_118, select_column)
        column_list = []
        for item in results:
            Extracted_flag = item['Extracted_flag']
            Column_Extraction_Deep = item['Column_Extraction_Deep']
            URL = item['URL']
            Title = item['Title']
            Domain_Code = item['Domain_Code']
            Host_Code = item['Host_Code']
            # 计算url MD5 先去掉http和末尾斜杆
            md5_source = URL
            md5_source = md5_source.replace('http://', '')
            md5_source = md5_source.replace('https://', '')
            md5_source = md5_source.rstrip('/')
            Record_MD5_ID = common.get_token(md5_source)
            Level_Score = item['Level_Score']
            Score_Detail = item['Score_Detail']
            Website_No = item['Website_No']
            Is_User_Added = item['Is_User_Added']
            column = f"('{Extracted_flag}', {Column_Extraction_Deep}, '{URL}', '{Title}', '{Domain_Code}', '{Host_Code}', '{Record_MD5_ID}', {Level_Score}, '{Score_Detail}', '{Website_No}', {Is_User_Added})"
            column_list.append(column)

        # 批量插入
        values = ",".join(column_list)
        insert_column = f"insert ignore into column_link(Extracted_flag, Column_Extraction_Deep, URL, Title, Domain_Code, Host_Code, Record_MD5_ID, Level_Score, Score_Detail, Website_No, Is_User_Added) values{values};"
        # print(insert_column)
        common.query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    for i in range(2179, 2182):
        print(i*10000, (i+1)*10000-1)
        main(i*10000, (i+1)*10000-1)
