# -*- coding: utf-8 -*-
from .sitemap import SitemapSpider


class SitemapRSpider(SitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': True,
    }
    name = 'sitemapr'
