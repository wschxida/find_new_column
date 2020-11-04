#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : output_data_root_to_column.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common


def main():
    extractor_118 = {'host': '192.168.1.133', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    # select_column = f"select Article_Current_Node_HTML, Website_URL from column_root_source where 1=1 " \
    #                 f"and Source='baidu_news';"
    select_column = f"select Website_URL,Website_Title,Website_Description,Website_Keywords from column_root_source " \
                    f"where 1=1 and Source='baidu_web' " \
                    f"and Website_Title not like '%新闻%' and Website_Title not like'%资讯%' and Website_Title not like'%论坛%' and Website_Title not like'%社区%'" \
                    f"and Website_Keywords like '%新闻%' and Website_Keywords like'%资讯%' and Website_Keywords like'%论坛%' and Website_Keywords like'%社区%';"

    try:
        results = common.query_mysql(extractor_118, select_column)
    except Exception as e:
        results = []

    column_list = []
    for i in results:
        title = i['Website_Title']
        listpage_url = i['Website_URL']

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

        try:
            title = title.split('-')[0].strip()
        except Exception as e:
            title = title
        try:
            title = title.split('_')[0].strip()
        except Exception as e:
            title = title
        try:
            title = title.split(',')[0].strip()
        except Exception as e:
            title = title
        try:
            title = title.split('，')[0].strip()
        except Exception as e:
            title = title
        try:
            title = title.split('|')[0].strip()
        except Exception as e:
            title = title
        try:
            title = title.split(' ')[0].strip()
        except Exception as e:
            title = title

        level_score = '100'
        Score_Detail = '"{\"status\": True, \"message\": \"root page\"}"'

        # website_no = 'BAIDU_NEWS'
        website_no = 'BAIDU_WEB'
        column_extraction_deep = 0
        column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{Score_Detail}')"
        # column_list.append(column)
        print(column)
        # 批量插入
        values = column
        # insert_column = f"replace into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, Score_Detail) values{values};"
        insert_column = f"insert ignore into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, Score_Detail) values{values};"
        # print(insert_column)
        try:
            common.query_mysql(extractor_118, insert_column)
        except Exception as e:
            pass


if __name__ == '__main__':
    main()
