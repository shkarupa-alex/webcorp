# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class KpSpider(scrapy.Spider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'ROBOTSTXT_OBEY': False,
        'REDIRECT_ENABLED': False,
    }
    name = 'kp'
    allowed_domains = ['www.kp.ru']
    start_urls = []

    url_template1 = 'https://www.kp.ru/daily/0/{}/'
    url_template2 = 'https://www.kp.ru/online/news/{}/'
    stop_post1 = 4176148  # 11.03.2020
    stop_post2 = 3792228  # 11.03.2020

    def __init__(self, *args, **kwargs):
        super(KpSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        max1, max2 = 1, 1
        for url in scraped_urls:
            id = int(url.split('/')[-2])
            if '/daily/' in url:
                max1 = max(max1, id)
            if '/online/' in url:
                max2 = max(max2, id)

        for p in range(max1, self.stop_post1):
            url = self.url_template1.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

        for p in range(max2, self.stop_post2):
            url = self.url_template2.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
