#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_detail_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import sys
sys.path.append('..')
import lib.common as common


def main():
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = f"""select ad.Article_URL, ad.Domain_Code, ad.Article_Source from article_detail ad
                    where 1=1 and ad.Website_No in (select Website_No from website where Website_Name like'%百度新闻%')
                    and ad.Extracted_Time>'2020-10-01' and ad.Extracted_Time<'2020-11-25'
                    and Article_Source is not NULL and Article_Source !='' GROUP BY Domain_Code;"""

    try:
        results = common.query_mysql(extractor_116, select_column)
    except Exception as e:
        results = []

    column_list = []
    for i in results:
        title = i['Article_Source']
        listpage_url = i['Article_URL']

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

        level_score = '100'
        Score_Detail = '"{\"status\": True, \"message\": \"root page\"}"'
        website_no = 'BAIDU_NEWS'
        column_extraction_deep = 0
        column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{Score_Detail}')"
        # column_list.append(column)
        print(column)
        # 批量插入
        values = column
        insert_column = f"replace into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, Score_Detail) values{values};"
        # print(insert_column)
        try:
            common.query_mysql(extractor_118, insert_column)
        except Exception as e:
            pass


if __name__ == '__main__':
    main()
