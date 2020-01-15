# -*- coding: utf-8 -*-
import scrapy
from six.moves.urllib.parse import urlparse
from slugify import slugify  # python-slugify


class SitemapSpider(scrapy.spiders.SitemapSpider):
    name = 'sitemap'
    sitemap_urls = [
        'https://lenta.ru/robots.txt',
        'https://roem.ru/robots.txt',
        'https://vc.ru/robots.txt',
        'https://www.kommersant.ru/robots.txt',
        'https://www.sport-express.ru/robots.txt',
        'https://zen.yandex.ru/robots.txt',
    ]
    custom_settings = {
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
    }

    def parse(self, response):
        url = response.url
        domain = urlparse(url).netloc
        html = response.text

        yield {
            '__csv_dump_name': slugify(domain),
            'url': url,
            'html': html
        }
