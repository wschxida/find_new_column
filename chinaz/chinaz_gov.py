#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : chinaz_gov.py
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
# url_template = "https://top.chinaz.com/hangye/index_gov_2.html"
url_template = "https://top.chinaz.com/hangye/index_gov_{}.html"


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
                    root = etree.HTML(text, parser=etree.HTMLParser(encoding='utf-8'))
                    print(url)

                    items = root.xpath('//*[@id="content"]/div[3]/div[3]/div[2]/ul/li')
                    # print(items)
                    for item in items:
                        title = "".join(item.xpath('./div[2]/h3/a/@title'))
                        print(title)
                        listpage_url = "".join(item.xpath('./div[2]/h3/span/text()'))
                        print(listpage_url)
                        listpage_url = "http://www." + listpage_url + "/"
                        listpage_url = listpage_url.replace('www.www.', 'www.')

                        es = Elasticsearch("192.168.2.56:9200")
                        _id = get_token(listpage_url)
                        data = {
                            "@timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.000+0800"),
                            "title": title,
                            "listpage_url": listpage_url
                        }
                        es.index(index="chinaz_gov", doc_type="chinaz_gov", id=_id, body=data)

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
        if i == 1:
            url = 'https://top.chinaz.com/hangye/index_gov.html'
        else:
            url = url_template.format(i)
        task = asyncio.ensure_future(get_response(url, semaphore))
        tasks.append(task)


def run():
    loop = asyncio.get_event_loop()
    for j in range(1, 108):
        print(j, j+1)
        global tasks
        tasks = []
        loop.run_until_complete(create_task(j, j+1))
        loop.run_until_complete(asyncio.gather(*tasks))
        time.sleep(2)
    loop.close()


if __name__ == '__main__':
    run()




