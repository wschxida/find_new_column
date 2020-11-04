#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : dongfanghao.py
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
# url_template = "http://dfhpc.dftoutiao.com/dfh_news_pc/getnews?authorname=&authorid=200000000000013&startcol=&newsnum=1&type=article"
url_template = "http://dfhpc.dftoutiao.com/dfh_news_pc/getnews?authorname=&authorid={}&startcol=&newsnum=1&type=article"


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
                    "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.113 Safari/537.36",
                }
                async with session.get(url, headers=headers) as response:
                    text = await response.text()
                    text = text.strip('null(').strip(')')
                    text_json = json.loads(text)
                    newslist = text_json["pglist"]
                    # print(text_json)
                    if len(newslist) > 0:
                        try:
                            # print(url)
                            source_name = text_json["authername"]
                            last_article_time = text_json["pglist"][0]["ts"]
                            url = url.replace("&newsnum=1&", "&newsnum=30&")
                            _id = get_token(url)

                            es = Elasticsearch("192.168.2.56:9200")
                            data = {
                                "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                                "title": source_name,
                                "listpage_url": url
                            }
                            es.index(index="dongfanghao_all", doc_type="dongfanghao", id=_id, body=data)
                            # 最近发布时间在2020年之后，2020/01/01 00:00:00
                            if int(last_article_time) > 1577836800000:
                                print(url)
                                es.index(index="dongfanghao", doc_type="dongfanghao", id=_id, body=data)
                                # return source_name, url

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
    # for i in range(5107773, 5107783):
    for i in range(start, end):
        url = url_template.format(200000000000000 + i)
        task = asyncio.ensure_future(get_response(url, semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    # for j in range(27, 1000000):
    for j in range(0, 300):
        print(1000*j, 1000*j+1000)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(1000*j-1, 1000*j+1000))
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()




