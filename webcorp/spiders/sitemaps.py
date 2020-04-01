# -*- coding: utf-8 -*-
from .sitemap import SitemapSpider


class SitemapSSpider(SitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOAD_DELAY': 4
    }
    name = 'sitemaps'
