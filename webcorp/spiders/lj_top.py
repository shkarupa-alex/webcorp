# -*- coding: utf-8 -*-
import scrapy


class LjTopSpider(scrapy.Spider):
    name = 'lj_top'
    allowed_domains = ['www.livejournal.com']
    start_urls = [
        # 'https://www.livejournal.com/'
    ]
    custom_settings = {
        'ROBOTSTXT_OBEY': False
    }

    csv_dump_name = 'livejournal_top'

    def __init__(self, *args, **kwargs):
        super(LjTopSpider, self).__init__(*args, **kwargs)

        users = [
            'https://www.livejournal.com/ratings/users/authority/?country=cyr&page={}'.format(p + 1)
            for p in range(3800)
        ]
        communities = [
            'https://www.livejournal.com/ratings/community/authority/?country=cyr&page={}'.format(p + 1)
            for p in range(190)
        ]
        self.start_urls = users + communities

    def parse(self, response):
        links = response.xpath('//a[contains(@class, "i-ljuser-username")]/@href').extract()
        for link in links:
            yield {
                '__csv_dump_name': self.csv_dump_name,
                'domain': link
            }
