# -*- coding: utf-8 -*-
import os
import scrapy
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
    }

    def __init__(self, *args, **kwargs):
        super(CcSpider, self).__init__(*args, **kwargs)

        part = kwargs.pop('part', None)
        if part is not None:
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
                    for row in f.read().split('\n'):
                        row = row.strip()
                        if not len(row):
                            continue
                        if row in scraped_urls:
                            continue
                        self.start_urls.append(row)
                        allowed_domains.add(urlparse(row).netloc)

            self.allowed_domains = list(allowed_domains)
            self.logger.info('Found {} users'.format(len(self.start_urls)))

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
