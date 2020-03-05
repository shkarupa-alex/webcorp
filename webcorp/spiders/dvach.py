# -*- coding: utf-8 -*-
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..common import hash_row, scraped_links


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
            allow=(r'.*\.html$', r'.*/$'),
            deny=(r'\/res\/\d+\.html$',)
        )),
        Rule(
            LinkExtractor(allow=(r'\/res\/\d+\.html$',)),
            callback='parse_item'
        ),
    )

    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        self.scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(self.scraped_urls)))

        super(DvachSpider, self).__init__(*args, **kwargs)

    def parse_item(self, response):
        # try:
        #     articles = response.xpath('//*[contains(@class, "post__message")]/text()')
        #     articles = [a.extract().strip() for a in articles]
        #     articles = [a for a in articles if len(a)]
        #     articles = '\n\n\n'.join(articles)
        #
        #     yield {
        #         'hash': hash_row([response.url, articles]),
        #         'url': response.url,
        #         'html': articles
        #     }
        # except:
        #     self.logger.warning('Error processing {}'.format(response.url))

        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
