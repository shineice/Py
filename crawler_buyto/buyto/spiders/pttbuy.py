# -*- coding: utf-8 -*-
from buyto.items import BuytoItem
import time
import scrapy

class PttbuySpider(scrapy.Spider):
    name = 'pttbuy'
    allowed_domains = ['ptt.cc']
    start_urls = ['https://www.ptt.cc/bbs/BuyTogether/index.html']

    def parse(self, response):
        for i in range(5000):
            url = "https://www.ptt.cc/bbs/BuyTogether/index" + str(4010 - i) + ".html"
            yield scrapy.Request (url, cookies={'over18': '1'}, callback=self.parse_article)

    def parse_article(self, response):
        item = BuytoItem()
        target = response.css("div.r-ent")


        for tag in target:
            try:
                item['title'] = tag.css("div.title a::text")[0].extract()
                item['author'] = tag.css('div.author::text')[0].extract()
                item['date'] = tag.css('div.date::text')[0].extract()
                item['push'] = tag.css('span::text')[0].extract()
                item['url'] = tag.css('div.title a::attr(href)')[0].extract()

                yield item

            except IndexError:
                pass
            continue