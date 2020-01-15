# -*- coding: utf-8 -*-
import scrapy


class LjUserSpider(scrapy.Spider):
    name = 'lj_user'
    allowed_domains = [
        # 'www.livejournal.com'
    ]
    start_urls = [
        # 'https://www.livejournal.com/'
    ]
    custom_settings = {
        'CSV_POOL_SUBDIR': 'livejournal.com'
    }
    # TODO prop_opt_readability=1

    def parse(self, response):
        pass
