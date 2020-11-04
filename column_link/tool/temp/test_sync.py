#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : test_sync.py
# @Author: Cedar
# @Date  : 2020/6/8
# @Desc  :

import time
import lib.common as common


def do_some_work(x):
    _title = '模拟IC'
    _listpage_url = 'https://news.sina.com.cn/'
    is_filter_status = common.is_need_filter(_title, _listpage_url, True, True)
    # time.sleep(1)
    print(is_filter_status)


now = lambda: time.time()


if __name__ == '__main__':
    start = now()
    print(start)

    for i in range(10000):
        coroutine = do_some_work(2)

    end = now()
    print(end)
    print(end - start)