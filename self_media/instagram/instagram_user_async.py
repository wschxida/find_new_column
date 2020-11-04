#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : instagram_user_async.py
# @Author: Cedar
# @Date  : 2020/4/17
# @Desc  :

import time
import asyncio, aiohttp
from aiohttp import ClientSession
import json
from datetime import datetime
from elasticsearch import Elasticsearch
import hashlib


tasks = []
# url_template = "https://i.instagram.com/api/v1/users/{0}/info/"
url_template = "https://i.instagram.com/api/v1/users/{0}/info/"
BASE_URL = 'https://www.instagram.com/'
STORIES_UA = 'Instagram 123.0.0.21.114 (iPhone; CPU iPhone OS 11_4 like Mac OS X; en_US; en-US; scale=2.00; 750x1334) AppleWebKit/605.1.15'
proxy = 'http://127.0.0.1:7777'


async def get_response(url, semaphore):
    async with semaphore:
        timeout = aiohttp.ClientTimeout(total=10)
        connector = aiohttp.TCPConnector(limit=60, verify_ssl=False)  # 60小于64。也可以改成其他数
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            try:
                headers = {
                    "Referer": BASE_URL,
                    "user-agent": STORIES_UA,
                }
                async with session.get(url, headers=headers, proxy=proxy) as response:
                    text = await response.text()
                    text_json = json.loads(text)
                    print(text_json)
                    try:
                        username = text_json["user"]["username"]
                        profile_pic_url = text_json["user"]["profile_pic_url"]
                        _id = int(text_json["user"]["pk"])

                        # 全入库
                        es = Elasticsearch("192.168.2.56:9200")
                        data = {
                            "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                            "username": username,
                            "id": _id,
                            "profile_pic_url": profile_pic_url
                        }
                        # es.index(index="baijiahao", doc_type="baijiahao", id=_id, body=data)
                        try:
                            es.create(index="instagram_user", doc_type="instagram_user", id=_id, body=data)
                        except Exception as e:
                            print(str(e))

                    except Exception as e:
                        print(str(e))
            # except aiohttp.client.ServerDisconnectedError as e:
            #     return str(e)
            # except aiohttp.client.ClientResponseError as e:
            #     return str(e)
            # except asyncio.TimeoutError as e:
            #     return str(e)
            # except aiohttp.client.ClientConnectorError as e:
            #     return str(e)
            except Exception as e:
                print(str(e))


async def create_task(start, end):
    semaphore = asyncio.Semaphore(100)  # 限制并发量为500
    for i in range(start, end):
        task = asyncio.ensure_future(get_response(url_template.format(i), semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    for j in range(0, 10):
        print(100*j, 100*j+100)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(100*j, 100*j+100))
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()




