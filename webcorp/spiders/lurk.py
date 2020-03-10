# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class LurkSpider(scrapy.Spider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'ROBOTSTXT_OBEY': False,
        'REDIRECT_ENABLED': False,
        # 'TOR_PROXY_ENABLED': True,
        # 'AUTOTHROTTLE_ENABLED': False,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
        # 'CONCURRENT_REQUESTS_PER_IP': 3,
    }
    name = 'lurk'
    allowed_domains = [
        'lurkmore.net'
    ]
    start_urls = [
        # 'https://lurkmore.to/%D0%A1%D0%BB%D1%83%D0%B6%D0%B5%D0%B1%D0%BD%D0%B0%D1%8F:AllPages'
    ]

    url_template = 'http://lurkmore.net/index.php?oldid={}&printable=yes'
    stop_post = 3000000  # 05.02.2020

    def __init__(self, *args, **kwargs):
        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        max_id = 1
        for url in scraped_urls:
            try:
                id = int(url.split('=')[1].split('&')[0])
            except:
                id = -1
            max_id = max(max_id, id)

        for p in range(max_id + 1, self.stop_post):
            url = self.url_template.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

        super(LurkSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
