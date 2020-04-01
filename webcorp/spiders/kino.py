# -*- coding: utf-8 -*-
import scrapy
from scrapy.exceptions import CloseSpider
from ..common import hash_row, scraped_links


class KinoSpider(scrapy.Spider):  # shows captcha
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'ROBOTSTXT_OBEY': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'REDIRECT_ENABLED': True,
    }
    name = 'kino'
    allowed_domains = ['www.kinopoisk.ru']
    start_urls = []

    url_template = 'https://www.kinopoisk.ru/film/{}/'
    stop_post = 420454  # 06.03.2020
    captcha = 0

    def __init__(self, *args, **kwargs):
        super(KinoSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        max_id = 1
        for url in scraped_urls:
            id = int(url.split('/')[-2])
            max_id = max(max_id, id)

        for p in range(max_id, self.stop_post):
            url = self.url_template.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        if 'capcha' in response.url or 'captcha' in response.url:
            self.captcha += 1

            if self.captcha > 10:
                raise CloseSpider('captcha')
        else:
            self.captcha = 0
            yield {
                'hash': hash_row([response.url, response.text]),
                'url': response.url,
                'html': response.text
            }
