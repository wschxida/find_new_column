#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : --2_update_cloud_website_no.py
# @Author: Cedar
# @Date  : 2020/4/21
# @Desc  :

import sys
sys.path.append('../..')
import lib.common as common


def main(website_no, start, end):
    extractor_116 = {'host': '192.168.1.116', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}

    # ID每增长1万个，编入同一个网站编号
    update_website = f"update cloud_listpage_url set Website_No='{website_no}' where Cloud_Listpage_URL_ID BETWEEN {start} AND {end};"
    print(update_website)

    try:
        result = common.query_mysql(extractor_116, update_website)
        print(result)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    num = 0
    for i in range(0, 1423):
        website = 'ST' + str(num).rjust(4, '0')
        num += 1
        print(website)
        print(i * 10000, (i + 1) * 10000 - 1)
        main(website, i*10000, (i+1)*10000-1)
