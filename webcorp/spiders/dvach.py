# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from ..storages import csv_joined_reader, check_row


class DvachSpider(CrawlSpider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
    }
    name = 'dvach'
    allowed_domains = [
        '2ch.hk'
    ]
    start_urls = [
        'https://2ch.hk/'
    ]
    rules = (
        Rule(LinkExtractor(
            allow=(r'.*',),
            deny=(r'\/res\/\d+\.html$',)
        )),
        Rule(
            LinkExtractor(allow=(r'\/res\/\d+\.html$',)),
            callback='parse_item'
        ),
    )

    csv_dump_name = '2ch'
    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        super(DvachSpider, self).__init__(*args, **kwargs)

        pool_path = get_project_settings().get('CSV_POOL_PATH')
        with csv_joined_reader(pool_path, self.csv_dump_name) as reader:
            for row in reader:
                if not check_row(row):
                    continue
                self.scraped_urls.add(row[1])
        self.logger.info('Already scraped {} urls'.format(len(self.scraped_urls)))

    def parse_item(self, response):
        articles = response.xpath('//*[contains(@class, "post__message")]/text()')
        articles = [a.extract().strip() for a in articles]
        articles = [a for a in articles if len(a)]
        articles = '\n\n\n'.join(articles)

        yield {
            '__csv_dump_name': self.csv_dump_name,
            'url': response.url,
            'html': articles
        }
