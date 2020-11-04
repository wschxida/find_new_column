#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : test_async.py
# @Author: Cedar
# @Date  : 2020/6/5
# @Desc  :

import asyncio
import time
from concurrent.futures import ThreadPoolExecutor
from asgiref.sync import sync_to_async
import lib.common as common


_executor = ThreadPoolExecutor()


def filter_column():
    _title = '模拟IC'
    _listpage_url = 'https://news.sina.com.cn/'
    is_filter_status = common.is_need_filter(_title, _listpage_url, True, True)
    # time.sleep(1)
    print(is_filter_status)


async def do_some_work():
    await loop.run_in_executor(_executor, filter_column)
    # await sync_to_async(filter_column)()


now = lambda: time.time()


if __name__ == '__main__':
    start = now()
    print(start)

    loop = asyncio.get_event_loop()
    tasks = []
    for i in range(10000):
        task = asyncio.ensure_future(do_some_work())
        tasks.append(task)
    loop.run_until_complete(asyncio.wait(tasks))

    end = now()
    print(end)
    print(end - start)
