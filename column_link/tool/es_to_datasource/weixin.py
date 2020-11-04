#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : weixin.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common
import warnings

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')


def main():
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'datasource'}

    select_column = '''
        SELECT
        lu.listpage_url,
        lu.listpage_title,
        w.Website_Important_Level
        FROM
            listpage_url lu
        LEFT JOIN task_schedule ts ON lu.website_no = ts.website_no
        LEFT JOIN website w ON lu.website_no = w.website_no
        WHERE
        ts.schedule_name IN (
            '搜狗微信_采集作者列表_Python脚本',
            '搜狗微信_列表_Python脚本_无数据')
    '''

    try:
        results = common.query_mysql(extractor_116, select_column)
        column_list = []
        count = 0
        print(len(results))
        for item in results:
            author_name = item['listpage_title']
            author_account = item['listpage_url'].replace('http://', '')
            author_id = author_account
            author_type = item['Website_Important_Level']
            is_added = 1
            column = f"('{author_name}', '{author_account}', '{author_id}', '{author_type}', {is_added})"
            column_list.append(column)

            # 每1万条插入一次，分批
            if count > 0 and not count % 10000:
                values = ",".join(column_list)
                insert_column = f"insert ignore into author_weixin(author_name, author_account, author_id, author_type, is_added) values{values};"
                # print(insert_column)
                common.query_mysql(extractor_118, insert_column)
                column_list = []
                print(count)

            count += 1

        values = ",".join(column_list)
        insert_column = f"insert ignore into author_weixin(author_name, author_account, author_id, author_type, is_added) values{values};"
        # print(insert_column)
        common.query_mysql(extractor_118, insert_column)

    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()



