#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : baijiahao.py
# @Author: Cedar
# @Date  : 2020/5/19
# @Desc  :


import time
import asyncio, aiohttp
from aiohttp import ClientSession
import json
import re
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib


tasks = []
# url_template = "https://baijiahao.baidu.com/u?app_id=1603499716255490"
url_template = "https://baijiahao.baidu.com/u?app_id={}"


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
        timeout = aiohttp.ClientTimeout(total=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            try:
                headers = {
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    text_json = re.findall(r"window.runtime= (.+?),window.runtime.pageType=", text)[0]
                    data = json.loads(text_json)
                    print(data)
                    if len(data) > 0:
                        try:
                            # print(url)
                            source_name = data["user"]["display_name"]
                            url = url
                            _id = get_token(url)
                            print(source_name)

                            es = Elasticsearch("192.168.2.56:9200")
                            data = {
                                "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                                "title": source_name,
                                "listpage_url": url
                            }
                            es.index(index="baijiahao_all", doc_type="baijiahao", id=_id, body=data)

                        except Exception as e:
                            print(e)
            # except aiohttp.client.ServerDisconnectedError as e:
            #     return str(e)
            # except aiohttp.client.ClientResponseError as e:
            #     return str(e)
            # except asyncio.TimeoutError as e:
            #     return str(e)
            # except aiohttp.client.ClientConnectorError as e:
            #     return str(e)
            except Exception as e:
                pass


async def create_task(start, end):
    semaphore = asyncio.Semaphore(1)  # 限制并发量为500
    for i in range(start, end):
        print(i)
        task = asyncio.ensure_future(get_response(url_template.format(i), semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    for j in range(1000, 100000):
        # print(1*j, 1*j+1)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(10*j, 10*j+10))
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()





