#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 3_insert_cloud_task_schedule.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import sys
sys.path.append('../..')
import lib.common as common


def main():
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}

    # ID每增长1万个，编入同一个网站编号
    insert_website = '''
        insert into cloud_task_schedule(Cloud_Server_Count,cloud_server_id,ListPage_URL_Count)
        select 63,cloud_server_id,count(1) from cloud_listpage_url
        GROUP BY cloud_server_id order by cloud_server_id;
    '''
    print(insert_website)

    try:
        common.query_mysql(extractor_116, insert_website)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
