#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : column_link_confirm.py
# @Author: Cedar
# @Date  : 2020/7/8
# @Desc  :


import time
import asyncio
import aiohttp
from lxml import etree
import lib.common as common
import warnings

# 忽略mysql插入时主键重复的警告
warnings.filterwarnings('ignore')
tasks = []


async def get_response(semaphore, url, title, domain_code_source, host_code_source, record_md5_id, website_no):
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
                    print(url)
                    items = root.xpath('//a')
                    valid_title_count = 0
                    for item in items:
                        _title = "".join(item.xpath('.//text()'))
                        if len(_title) > 10:
                            valid_title_count += 1

                    score, status = common.is_need_filter(title, url, False)
                    level_score = score + valid_title_count
                    score_detail = f'score={score},valid_title_count={valid_title_count}'

                    column = f"('{title}', '{url}', '{domain_code_source}', '{host_code_source}', '{record_md5_id}', '{website_no}', {level_score}, '{score_detail}')"
                    return column

            except Exception as e:
                print(e)


async def create_task(start, end):
    semaphore = asyncio.Semaphore(200)  # 限制并发量为500

    # 查询待采集目标
    results = None
    extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db', 'db': 'mymonitor'}
    # website_no = ('', '01-114', '')
    select_column = f"select Column_Link_ID, URL, title, Domain_Code, host_code, record_md5_id, Website_No, Is_System_Confirmed  " \
                    f"from column_link where Website_No !='GUOWAI' and Is_System_Confirmed=0 order by Record_MD5_ID limit 100"
    print(select_column)
    try:
        results = common.query_mysql(extractor_118, select_column)
        # 更新Extracted_flag
        id_list = [0]
        for result in results:
            id_list.append(result["Column_Link_ID"])
        id_list = tuple(id_list)
        update_flag = f"update column_link set Is_System_Confirmed=1 where Column_Link_ID in {id_list};"
        # print(update_flag)
        try:
            common.query_mysql(extractor_118, update_flag)
        except Exception as e:
            pass

        # 开始任务
        for result in results:
            # print(result)
            url = result["URL"]
            title = result["title"]
            domain_code = result["Domain_Code"]
            host_code = result["host_code"]
            record_md5_id = result["record_md5_id"]
            website_no = result["Website_No"]
            task = asyncio.ensure_future(
                get_response(semaphore, url, title, domain_code, host_code, record_md5_id, website_no))
            tasks.append(task)
    except Exception as e:
        pass


def run():
    for j in range(1, 200000):
        print(j, j + 1)
        print(time.time())
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(j, j + 1))
        results = loop.run_until_complete(asyncio.gather(*tasks))
        column_list = []
        for result in results:
            if result:
                column_list.append(result)
        values = ",".join(column_list)
        insert_column = f"insert ignore into column_link_tmp(Title, URL, domain_code, host_code, record_md5_id, website_no, level_score, score_detail) " \
                        f"values{values};"
        # print(insert_column)
        # 批量插入
        extractor_118 = {'host': '192.168.1.118', 'port': 3306, 'user': 'root', 'passwd': 'poms@db',
                         'db': 'mymonitor'}
        try:
            common.query_mysql(extractor_118, insert_column)
        except Exception as e:
            print(e)
        time.sleep(2)
        loop.close()


if __name__ == '__main__':
    run()
