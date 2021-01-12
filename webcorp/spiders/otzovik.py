# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..common import hash_row, scraped_links


class OtzovikSpider(CrawlSpider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': True,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,

        'DOWNLOAD_DELAY': 10,
    }
    name = 'otzovik'
    allowed_domains = [
        'otzovik.com'
    ]
    start_urls = [
        'http://otzovik.com/sitemap/'
    ]
    rules = (
        Rule(
            LinkExtractor(allow=(r'/reviews/',)),
            callback='parse_item'
        ),
        Rule(LinkExtractor(
            allow=(r'.*/$',),
            deny=(r'/reviews/',)
        )),
    )

    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        self.scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(self.scraped_urls)))

        super(OtzovikSpider, self).__init__(*args, **kwargs)

    def parse_item(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
