#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : 4_cloud_task_schedule.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :


import lib.common as common


def main(start_server_id, end_server_id, count):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}

    for i in range(start_server_id, end_server_id):
        print(i)
        update_website = f'''
            update cloud_task_schedule set cloud_server_id={i} where schedule_id>2310 
            and schedule_id in (select * from 
            (select Schedule_ID from cloud_task_schedule where Schedule_ID>2310 and Cloud_Server_ID is null order by Schedule_ID limit {count})aa
            );
        '''
        print(update_website)
        try:
            common.query_mysql(extractor_116, update_website)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    main(0, 11, 11)
    main(11, 63, 12)

# 公式
# 745 / 63 = 11.8
# 11x + 12(63-x) = 745
# x = 11
# 745 = 11*11 + (63-11)*12
