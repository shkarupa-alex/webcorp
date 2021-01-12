# -*- coding: utf-8 -*-
import json
import os
import scrapy
from scrapy.utils.project import get_project_settings
from ..common import hash_row, scraped_links


class OtvetSpider(scrapy.Spider):
    name = 'otvet'
    allowed_domains = ['otvet.mail.ru']
    start_urls = [
        # 'https://otvet.mail.ru/'
    ]

    url_template = 'https://otvet.mail.ru/api/v2/question?qid={}'
    fs_links = None
    skip_lines = 0

    def __init__(self, *args, **kwargs):
        super(OtvetSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        scraped_urls_ = set()
        storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
        for storage in storage_paths:
            if not os.path.exists(storage):
                continue
            feed = os.path.join(storage, 'fs_links', 'sitemap_14.txt'.format(id))
            if not os.path.exists(feed):
                continue
            self.fs_links = feed
            break

        if self.fs_links is None:
            self.logger.warning('Not found links to scrape')
            return

        if len(scraped_urls):
            with open(self.fs_links, 'rt') as f:
                line = 0
                for row in f:
                    line += 1
                    row = row.strip()
                    if not len(row):
                        continue
                    if row in scraped_urls or row in scraped_urls_:
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
                if not len(row) or '/' not in row:
                    continue

                id = int(row.split('/')[-1])
                if id < 1:
                    continue

                yield scrapy.Request(self.url_template.format(id), dont_filter=True)

    def parse(self, response):
        result = json.loads(response.text)

        if 'qid' in result:
            qid = result['qid']
        else:
            qid = int(response.url.split('=')[-1])
        url = 'https://otvet.mail.ru/question/{}'.format(qid)

        if 'error' in result:
            yield {
                'hash': hash_row([url, '']),
                'url': url,
                'html': ''
            }
        else:
            text = result['qtext'] + '\n' + result['qcomment']
            if 'best' in result:
                text += '\n\n' + result['best']['atext']

            for a in result['answers']:
                text += '\n\n' + a['atext']
            for a in result['comments']:
                text += '\n\n' + a['cmtext']

            yield {
                'hash': hash_row([url, text]),
                'url': url,
                'html': text
            }
