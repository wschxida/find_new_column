#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : qiluyidianhao.py
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
from lxml import etree


tasks = []
# url_template = "http://www.ql1d.com/channel/index/ownerid/206"
url_template = "http://www.ql1d.com/channel/index/ownerid/{}"


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
                    # response.encoding = "utf-8"
                    root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))

                    article_url = "".join(root.xpath('//*[@id="content-list"]/div[2]/div/div[1]/a/@href'))
                    article_url_id = int(article_url.strip(".html").strip("/news/show/id/"))
                    print(article_url_id)

                    # print(text_json)
                    # http://www.ql1d.com/news/show/id/11503356.html
                    if article_url_id > 11503356:
                        try:
                            print(url)
                            source_name = "".join(root.xpath('//*[@id="content-list"]/div[1]/div/div/div[2]/div[1]/text()'))
                            es = Elasticsearch("192.168.2.56:9200")
                            _id = get_token(url)
                            data = {
                                "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                                "title": source_name,
                                "listpage_url": url
                            }
                            es.index(index="qiluyidianhao", doc_type="qiluyidianhao", id=_id, body=data)
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
    semaphore = asyncio.Semaphore(10)  # 限制并发量为500
    # for i in range(5107773, 5107783):
    for i in range(start, end):
        task = asyncio.ensure_future(get_response(url_template.format(i + 1), semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    for j in range(0, 100):
        print(1000*j, 1000*j+1000)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(1000*j-1, 1000*j+1000))
        loop.run_until_complete(asyncio.gather(*tasks))
    loop.close()


if __name__ == '__main__':
    run()




