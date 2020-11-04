#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : yidianhao.py
# @Author: Cedar
# @Date  : 2020/4/17
# @Desc  :

import time
import asyncio, aiohttp
from aiohttp import ClientSession
import re
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib


tasks = []
# url_template = "http://www.yidianzixun.com/channel/m324649"
url_template = "http://www.yidianzixun.com/channel/m{}"


def get_token(md5str):
    # md5str = "abc"
    # 生成一个md5对象
    m1 = hashlib.md5()
    # 使用md5对象里的update方法md5转换
    m1.update(md5str.encode("utf-16LE"))
    token = m1.hexdigest()
    return token


async def get_response(url, semaphore):
    async with semaphore:
        async with aiohttp.ClientSession() as session:
            try:
                headers = {
                    'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 11_0 like Mac OS X) AppleWebKit/604.1.38 (KHTML, like Gecko) Version/11.0 Mobile/15A372 Safari/604.1',
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    json_text = re.findall('yidian.docinfo = ({.*});</script>', text)[0]
                    result = json.loads(json_text)
                    source_name = result["channel_name"]

                    if len(source_name) > 0:
                        try:
                            print(url)
                            _id = get_token(url)

                            es = Elasticsearch("192.168.2.56:9200")
                            data = {
                                "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                                "title": source_name,
                                "listpage_url": url
                            }
                            es.index(index="yidianhao_all", doc_type="yidianhao", id=_id, body=data)

                        except Exception as e:
                            print(e)

            except Exception as e:
                pass


async def create_task(start, end):
    semaphore = asyncio.Semaphore(10)  # 限制并发量为500
    for i in range(start, end):
        task = asyncio.ensure_future(get_response(url_template.format(i + 1), semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    # for j in range(26953, 100000):
    for j in range(0, 10000):
        print(1000*j, 1000*j+1000)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(1000*j-1, 1000*j+1000))
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()




