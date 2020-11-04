#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : start.py
# @Author: Cedar
# @Date  : 2020/4/16
# @Desc  :

from scrapy import cmdline

cmdline.execute("scrapy crawl gov".split())
