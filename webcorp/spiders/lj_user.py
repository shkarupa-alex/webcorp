# -*- coding: utf-8 -*-
import json
import os
import re
from six.moves.urllib.parse import urlparse
from slugify import slugify  # python-slugify
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from ..common import cleanup, extract
from ..storages import csv_joined_reader, check_row


class LjUserSpider(CrawlSpider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'CSV_POOL_SUBDIR': 'livejournal.com',
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1
    }

    name = 'lj_user'
    allowed_domains = [
        'livejournal.com'
    ]
    start_urls = [
        # 'https://www.livejournal.com/'
    ]
    rules = (
        Rule(LinkExtractor(
            allow=(r'\/\?skip=\d+$',)
        )),
        Rule(
            LinkExtractor(
                allow=(r'\/\d+\.html(#[^&?\/]+)?$',)
            ),
            callback='parse_item'
        ),
    )

    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        super(LjUserSpider, self).__init__(*args, **kwargs)

        pool_path = get_project_settings().get('CSV_POOL_PATH')
        with csv_joined_reader(pool_path, 'livejournal_top') as reader:
            for row in reader:
                if not check_row(row):
                    continue
                self.start_urls.append(row[1])
        self.logger.info('Found {} users'.format(len(self.start_urls)))

        userid = int(kwargs.pop('userid', -1))
        if userid < 0:
            self.start_urls = []
        else:
            self.start_urls = self.start_urls[userid: userid + 1]

        users_path = os.path.join(pool_path, self.custom_settings['CSV_POOL_SUBDIR'])
        os.makedirs(users_path, exist_ok=True)
        for url in self.start_urls:
            domain = urlparse(url).netloc
            self.allowed_domains.append(domain)
            csv_dump_name = slugify(domain)
            with csv_joined_reader(users_path, csv_dump_name) as reader:
                for row in reader:
                    if not check_row(row):
                        continue
                    self.scraped_urls.add(row[1])
        self.logger.info('Already scraped {} urls'.format(len(self.scraped_urls)))

    def parse_item(self, response):
        domain = urlparse(response.url).netloc
        csv_dump_name = slugify(domain)

        # comments = []
        # try:
        #     meta_text = re.findall(r'\sSite\.page = (\{")([\s\S]+?)(\});\s+Site', response.text)
        #     if len(meta_text) == 1 and len(meta_text[0]) == 3:
        #         meta_dict = json.loads(''.join(meta_text[0]))
        #         if 'comments' in meta_dict:
        #             for c in meta_dict['comments']:
        #                 if 'uname' not in c or 'bot' in c['uname']:
        #                     continue
        #                 if 'article' not in c or not c['article']:
        #                     continue
        #                 comments.append(c['article'])
        # except Exception as e:
        #     self.logger.info(e)
        # comments = '\n\n\n'.join(comments)
        #
        # text = extract(cleanup(response.text)) + '\n' * 10 + comments
        text = response.text

        yield {
            '__csv_dump_name': csv_dump_name,
            'url': response.url,
            'html': text
        }
