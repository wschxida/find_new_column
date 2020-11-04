#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_oversea.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common


def main():
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}
    select_column = f"select * from website where ID>1577;"

    try:
        results = common.query_mysql(extractor_118, select_column)
        column_list = []
        for item in results:
            ID = item['ID']
            Column_Extraction_Deep = '0'
            URL = item['URL']
            Title = item['Title']
            Domain_Code = common.get_domain_code(URL)
            Host_Code = common.get_host_code(URL)
            # 计算url MD5 先去掉http和末尾斜杆
            md5_source = URL
            md5_source = md5_source.replace('http://', '')
            md5_source = md5_source.replace('https://', '')
            md5_source = md5_source.rstrip('/')
            Record_MD5_ID = common.get_token(md5_source)
            Level_Score = '100'
            Score_Detail = '{"status": True, "message": "root page"}'
            Website_No = 'OVERSEA'
            column = f"({Column_Extraction_Deep}, '{URL}', '{Title}', '{Domain_Code}', '{Host_Code}', '{Record_MD5_ID}', {Level_Score}, '{Score_Detail}', '{Website_No}')"
            column_list.append(column)
            # # 更新md5
            update_website = f"update website set record_md5_id='{Record_MD5_ID}' where ID={ID}"
            common.query_mysql(extractor_118, update_website)
            insert_column = f"insert ignore into column_link_oversea(Column_Extraction_Deep, URL, Title, Domain_Code, Host_Code, Record_MD5_ID, Level_Score, Score_Detail, Website_No) values{column};"
            print(insert_column)
            common.query_mysql(extractor_118, insert_column)

        # # 批量插入
        # values = ",".join(column_list)
        # insert_column = f"insert ignore into column_link_oversea(Column_Extraction_Deep, URL, Title, Domain_Code, Host_Code, Record_MD5_ID, Level_Score, Score_Detail, Website_No) values{values};"
        # print(insert_column)
        # common.query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
