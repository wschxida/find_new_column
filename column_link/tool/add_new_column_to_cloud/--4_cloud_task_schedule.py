#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : --4_cloud_task_schedule.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import sys
sys.path.append('../..')
import lib.common as common


def main(start_server_id, end_server_id, count):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}

    for i in range(start_server_id, end_server_id):
        print(i)
        update_website = f'''
            update cloud_task_schedule set cloud_server_id={i} where 1=1 
            and schedule_id in (select * from 
            (select Schedule_ID from cloud_task_schedule where 1=1 and Cloud_Server_ID is null order by Schedule_ID limit {count})aa
            );
        '''
        print(update_website)
        try:
            common.query_mysql(extractor_116, update_website)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main(10000, 10026, 22)
    main(10026, 10063, 23)

# 公式
# 1423 / 63 = 22.5
# 22x + 23(63-x) = 1423
# x = 26
# 1423 = 22*26 + 23*(63-26)
