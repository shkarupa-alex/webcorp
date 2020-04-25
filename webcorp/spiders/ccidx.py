# -*- coding: utf-8 -*-
import os
import scrapy
from scrapy.utils.project import get_project_settings
from six.moves.urllib.parse import urlparse
from ..common import hash_row


class CcIdxSpider(scrapy.Spider):
    name = 'ccidx'
    allowed_domains = []
    start_urls = []
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'CONCURRENT_REQUESTS_PER_IP': 4,
        'ROBOTSTXT_OBEY': False
    }

    def __init__(self, *args, **kwargs):
        super(CcIdxSpider, self).__init__(*args, **kwargs)

        allowed_domains, start_urls = set(), set()
        storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
        for storage in storage_paths:
            links = os.path.join(storage, 'cc_links')
            if not os.path.exists(storage) or not os.path.exists(links):
                continue

            feeds = [f for f in os.listdir(links) if f.endswith('.txt')]
            for feed in feeds:
                with open(os.path.join(links, feed), 'rt') as f:
                    for row in f.read().split('\n'):
                        row = row.strip()
                        if not len(row):
                            continue
                        parsed = urlparse(row)
                        allowed_domains.add(parsed.netloc)
                        start_urls.add(parsed.scheme + '://' + parsed.netloc + '/')
                break

        self.allowed_domains = list(allowed_domains)
        self.start_urls = list(start_urls)
        self.logger.info('Start with {} urls'.format(len(self.start_urls)))

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
