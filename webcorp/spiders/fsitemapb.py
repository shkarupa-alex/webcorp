# -*- coding: utf-8 -*-
from .fsitemap import FSitemapSpider


class FSitemapBSpider(FSitemapSpider):
    name = 'fsitemapb'

    def response_is_ban(self, request, response):
        return False

    def exception_is_ban(self, request, exception):
        return None
