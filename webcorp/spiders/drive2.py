# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..common import hash_row, scraped_links


class Drive2Spider(CrawlSpider):
    custom_settings = {
        'DEPTH_LIMIT': 5
        #     'DEPTH_PRIORITY': 1,
        #     'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        #     'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue'
    }
    name = "drive2"
    allowed_domains = [
        'www.drive2.ru'
    ]
    start_urls = [
        'https://www.drive2.ru/cars/',
        'https://www.drive2.ru/featured-topics/'
    ]
    rules = (
        Rule(LinkExtractor(
            allow=(
                r'\/cars\/[^?]*$',
                r'\/experience\/[^?]*$',
                r'\/experience\/[^?]*\?to=\d+$',
                r'\/featured-topics\/[^?]*$',
                r'\/topics\/[^?]*$',
                r'\/topics\/[^?]*\?to=\d+$',
            ),
            deny=(
                r'\/communities',
                r'\/market',
                '\/editorial',
                '\/communities\/',
            )
        )),
        Rule(
            LinkExtractor(allow=(
                r'\/l\/\d+\/$',
                r'\/b\/\d+\/$',
                r'\/r\/.+\/\d+\/$',
            )),
            callback='parse_item',
            follow=True
        ),
    )
    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        super(Drive2Spider, self).__init__(*args, **kwargs)

        self.scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(self.scraped_urls)))

    def parse_item(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
