# -*- coding: utf-8 -*-
import scrapy
from scrapy import Request
from ..common import hash_row, scraped_links


class AskSpider(scrapy.Spider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'REDIRECT_ENABLED': False,
    }
    name = 'ask'
    allowed_domains = ['ask.fm']
    start_urls = [
        # 'https://ask.fm/'
    ]

    url_template = 'https://ask.fm/id{}'
    start_post = 1
    stop_post = 50000000

    def __init__(self, *args, **kwargs):
        super(AskSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        for url in scraped_urls:
            id = int(url.split('/id')[-1])
            self.start_post = max(self.start_post, id)

    def start_requests(self):
        for p in range(self.start_post, self.stop_post):
            url = self.url_template.format(p)
            yield Request(url, dont_filter=True)

    def parse(self, response):
        if 'html lang="ru" ' not in response.text:
            self.logger.debug('Skip "en" url {}'.format(response.url))
            yield {
                'hash': hash_row([response.url, '']),
                'url': response.url,
                'html': ''
            }
        if 'class="rsp-container missing-content"' in response.text \
                or 'This account has been suspended' in response.text:
            self.logger.debug('Skip "disabled" profile {}'.format(response.url))
            yield {
                'hash': hash_row([response.url, '']),
                'url': response.url,
                'html': ''
            }

        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
