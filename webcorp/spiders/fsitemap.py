# -*- coding: utf-8 -*-
import os
import scrapy
from scrapy.utils.project import get_project_settings
from ..common import hash_row, scraped_links


class FSitemapSpider(scrapy.Spider):
    custom_settings = {
        'REDIRECT_ENABLED': True,
    }
    name = 'fsitemap'
    allowed_domains = ['zen.yandex.ru', 'irecommend.ru', 'otvet.mail.ru', 'pikabu.ru']
    start_urls = []

    fs_links = None
    skip_lines = 0

    def __init__(self, *args, **kwargs):
        super(FSitemapSpider, self).__init__(*args, **kwargs)

        id = int(kwargs.pop('urlid', -1))
        if id < 0:
            self.logger.warning('Wrong "urlid"')
            return

        scraped_urls = scraped_links('sitemap_{}'.format(id))
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
        for storage in storage_paths:
            if not os.path.exists(storage):
                continue
            feed = os.path.join(storage, 'fs_links', 'sitemap_{}.txt'.format(id))
            if not os.path.exists(feed):
                continue
            self.fs_links = feed
            break

        if self.fs_links is None:
            self.logger.warning('Not found links to scrape')
            return

        with open(self.fs_links, 'rt') as f:
            line = 0
            for row in f:
                line += 1
                row = row.strip()
                if not len(row):
                    continue
                if row in scraped_urls:
                    self.skip_lines = line

        del scraped_urls

        self.logger.info('Will skip {} lines'.format(self.skip_lines))

    def start_requests(self):
        if self.fs_links is None:
            return

        with open(self.fs_links, 'rt') as f:
            line = 0
            for row in f:
                line += 1
                if line < self.skip_lines:
                    continue
                row = row.strip()
                if not len(row):
                    continue

                yield scrapy.Request(row, dont_filter=True)

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
