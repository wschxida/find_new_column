# -*- coding: utf-8 -*-
import scrapy
from govSpider.items import GovWesbiteItem


class Opp2Spider(scrapy.Spider):
    name = 'gov'
    allowed_domains = []
    start_urls = ['http://www.gov.cn/']

    def parse(self, response):

        for each in response.xpath("/html/body//a"):

            item = GovWesbiteItem()
            # extract()方法返回的都是unicode字符串
            title = each.xpath("text()").extract()
            url = each.xpath("@href").extract()

            # xpath返回的是包含一个元素的列表
            item['title'] = "".join(title).strip()
            item['url'] = "".join(url)

            is_yield = True
            if len(item['title']) < 1:
                continue
            if len(item['title']) > 10:
                continue
            if "gov.cn" not in item['url']:
                continue
            if "javascript" in item['url']:
                continue
            if "htm" in item['url']:
                if "index" in item['url']:
                    is_yield = True
                else:
                    continue

            if is_yield:
                yield item


