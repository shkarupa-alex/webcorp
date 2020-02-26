# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from ..common import cleanup, extract
from ..storages import csv_joined_reader, check_row


class Drive2Spider(CrawlSpider):
    custom_settings = {
        'DEPTH_PRIORITY': 1,
        'SCHEDULER_DISK_QUEUE': 'scrapy.squeues.PickleFifoDiskQueue',
        'SCHEDULER_MEMORY_QUEUE': 'scrapy.squeues.FifoMemoryQueue'
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
    csv_dump_name = 'drive2'

    def __init__(self, *args, **kwargs):
        super(Drive2Spider, self).__init__(*args, **kwargs)

        pool_path = get_project_settings().get('CSV_POOL_PATH')
        with csv_joined_reader(pool_path, self.csv_dump_name) as reader:
            for row in reader:
                if not check_row(row):
                    continue
                self.scraped_urls.add(row[1])

    def parse_item(self, response):
        # text = extract(cleanup(response.text))
        text = response.text

        yield {
            '__csv_dump_name': self.csv_dump_name,
            'url': response.url,
            'html': text
        }
