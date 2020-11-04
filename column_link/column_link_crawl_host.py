#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : column_link_crawl_host.py
# @Author: Cedar
# @Date  : 2020/4/22
# @Desc  :

import time
import asyncio
import aiohttp
from aiohttp import ClientSession
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib
from lxml import etree
from requests.compat import urljoin
import pymysql
from lib import common
import warnings

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
tasks = []


async def get_response(semaphore, url, column_extraction_deep=1, domain_code_source=None, website_no=None):
    async with semaphore:
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=60, verify_ssl=False)  # 60小于64。也可以改成其他数
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            try:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
                    # print(url)
                    # column_extraction_deep = int(column_extraction_deep) + 1
                    column_extraction_deep = 1

                    items = root.xpath('//a')
                    column_list = []
                    # print(len(items))
                    for item in items:
                        title = "".join(item.xpath('.//text()'))
                        listpage_url = "".join(item.xpath('./@href'))
                        listpage_url = urljoin(url, listpage_url)
                        # 去掉标点符号
                        title = common.filter_punctuation(title)

                        domain_code = common.get_domain_code(listpage_url)
                        host_code = common.get_host_code(listpage_url)
                        host_code_index = listpage_url.index(host_code) + len(host_code)
                        listpage_url = listpage_url[0:host_code_index] + '/'
                        # 计算url MD5 先去掉http和末尾斜杆
                        md5_source = listpage_url
                        md5_source = md5_source.replace('http://', '')
                        md5_source = md5_source.replace('https://', '')
                        md5_source = md5_source.rstrip('/')
                        record_md5_id = common.get_token(md5_source)
                        # domain 要与源域名一致
                        # if domain_code_source != domain_code:
                        #     continue

                        # 垃圾词、垃圾域名过滤
                        level_score, score_detail = common.is_need_filter(title, listpage_url, False)
                        print(level_score, score_detail)
                        if level_score > -100:
                            column = f"('{title}', '{listpage_url}', '{record_md5_id}', '{website_no}', {column_extraction_deep}, '{domain_code}', '{host_code}', '{level_score}', '{score_detail}')"
                            column_list.append(column)

                    # 批量插入
                    extractor_118 = {'host': '192.168.1.133', 'port': 3306, 'user': 'root', 'passwd': 'poms@db',
                                     'db': 'mymonitor'}
                    values = ",".join(column_list)
                    insert_column = f"insert ignore into column_link(Title, URL, record_md5_id, website_no, column_extraction_deep, domain_code, host_code, level_score, score_detail) values{values};"
                    # print(insert_column)
                    try:
                        common.query_mysql(extractor_118, insert_column)
                    except Exception as e:
                        pass

            except Exception as e:
                print(e)


async def create_task(start, end):
    semaphore = asyncio.Semaphore(20)  # 限制并发量为500

    # 查询待采集目标
    results = None
    extractor_118 = {'host': '192.168.1.133', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    # website_no = ('', '01-114', '')
    select_column = f"select Column_Link_ID, Column_Extraction_Deep, URL, Domain_Code, Website_No, Extracted_flag " \
                    f"from column_link where website_no='0728' and Extracted_flag is null " \
                    f"ORDER BY Column_Extraction_Deep limit 100;"
    print(select_column)
    try:
        results = common.query_mysql(extractor_118, select_column)
        # 更新Extracted_flag
        id_list = [0]
        for result in results:
            id_list.append(result["Column_Link_ID"])
        id_list = tuple(id_list)
        update_flag = f"update column_link set Extracted_flag='S' where Column_Link_ID in {id_list};"
        # print(update_flag)
        try:
            common.query_mysql(extractor_118, update_flag)
        except Exception as e:
            pass

        # 开始任务
        for result in results:
            # print(result)
            url = result["URL"]
            domain_code = result["Domain_Code"]
            website_no = result["Website_No"]
            column_extraction_deep = result["Column_Extraction_Deep"]
            if not column_extraction_deep:
                column_extraction_deep = 0
            # 最多采集3层
            if column_extraction_deep <= 3:
                task = asyncio.ensure_future(
                    get_response(semaphore, url, column_extraction_deep, domain_code, website_no))
                tasks.append(task)
    except Exception as e:
        pass


def run():
    for j in range(1, 200000):
        print(j, j + 1)
        print(time.time())
        # loop = asyncio.new_event_loop()
        loop = asyncio.get_event_loop()
        # asyncio.set_event_loop(loop)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(j, j + 1))
        loop.run_until_complete(asyncio.gather(*tasks))
        # loop.close()
        time.sleep(2)


if __name__ == '__main__':
    run()
