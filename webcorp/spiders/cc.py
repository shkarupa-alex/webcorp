# -*- coding: utf-8 -*-
import os
import scrapy
from scrapy import Request
from scrapy.utils.project import get_project_settings
from six.moves.urllib.parse import urlparse
from ..common import hash_row, scraped_links


class CcSpider(scrapy.Spider):
    name = 'cc'
    allowed_domains = []
    start_urls = []
    custom_settings = {
        'CONCURRENT_REQUESTS_PER_DOMAIN': 4,
        'CONCURRENT_REQUESTS_PER_IP': 4,
        'ROBOTSTXT_OBEY': False
    }
    skip_lines = 0
    links_part = None

    def __init__(self, *args, **kwargs):
        super(CcSpider, self).__init__(*args, **kwargs)

        part = kwargs.pop('part', None)
        if part is None:
            return

        self.links_part = part
        allowed_domains = set()

        scraped_urls = scraped_links(self.name + '_' + part)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
        for storage in storage_paths:
            if not os.path.exists(storage):
                continue
            feed = os.path.join(storage, 'cc_links', part + '.txt')
            if not os.path.exists(feed):
                continue
            with open(feed, 'rt', newline='') as f:
                for i, row in enumerate(f.read().split('\n')):
                    row = row.strip()
                    if not len(row):
                        continue
                    if row in scraped_urls:
                        self.skip_lines = i
                    allowed_domains.add(urlparse(row).netloc)
            break

        self.allowed_domains = list(allowed_domains)
        self.logger.info('Will skip {} lines'.format(self.skip_lines))

    def start_requests(self):
        storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
        for storage in storage_paths:
            if not os.path.exists(storage):
                continue
            feed = os.path.join(storage, 'cc_links', self.links_part + '.txt')
            if not os.path.exists(feed):
                continue
            with open(feed, 'rt', newline='') as f:
                i = 0
                for row in f:
                    i += 1
                    if i < self.skip_lines:
                        continue
                    row = row.strip()
                    if not len(row):
                        continue
                    yield Request(row, dont_filter=True)
            break

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }

    def response_is_ban(self, request, response):
        return False

    def exception_is_ban(self, request, exception):
        return None
