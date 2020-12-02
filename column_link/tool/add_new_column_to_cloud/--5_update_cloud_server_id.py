#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : --5_update_cloud_server_id.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import sys
sys.path.append('../..')
import lib.common as common


def main():
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}

    select_sql = 'select cloud_server_id,website_no from cloud_task_schedule;'

    try:
        results = common.query_mysql(extractor_116, select_sql)
        for item in results:
            cloud_server_id = item['cloud_server_id']
            website_no = item['website_no']
            print(cloud_server_id, website_no)
            update_cloud = f"update cloud_listpage_url set Cloud_Server_ID={cloud_server_id} where website_no='{website_no}';"
            print(update_cloud)
            common.query_mysql(extractor_116, update_cloud)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    main()
