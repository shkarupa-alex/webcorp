# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class KommersantSpider(scrapy.Spider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'ROBOTSTXT_OBEY': False,
        'REDIRECT_ENABLED': False,
    }
    name = 'kommersant'
    allowed_domains = ['www.kommersant.ru']
    start_urls = []

    url_template = 'https://www.kommersant.ru/doc/{}'
    stop_post = 4274597  # 02.03.2020

    def __init__(self, *args, **kwargs):
        super(KommersantSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        max_id = 1
        for url in scraped_urls:
            id = int(url.split('/')[-1])
            max_id = max(max_id, id)

        # for p in range(1, self.stop_post):
        for p in range(max_id, self.stop_post):
            url = self.url_template.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
