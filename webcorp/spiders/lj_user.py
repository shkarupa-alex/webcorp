# -*- coding: utf-8 -*-
import csv
import gzip
import os
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from scrapy.utils.project import get_project_settings
from six.moves.urllib.parse import urlparse
from ..common import hash_row, scraped_links


class LjUserSpider(CrawlSpider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        'DOWNLOAD_DELAY': 5
    }

    name = 'lj_user'
    allowed_domains = [
        # 'livejournal.com'
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
        storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
        for storage in storage_paths:
            if not os.path.exists(storage):
                continue
            feed = os.path.join(storage, 'livejournal_top-000000000.csv.gz')
            if not os.path.exists(feed):
                continue
            with gzip.open(feed, 'rt', newline='') as f:
                reader = csv.reader(f)
                for row in reader:
                    if len(row) != 2:
                        raise KeyError('Wrong feed format')
                    self.start_urls.append(row[1])
        self.logger.info('Found {} users'.format(len(self.start_urls)))

        userid = int(kwargs.pop('userid', -1))
        # self.logger.info('---userid: {}'.format(userid))
        # self.logger.info('---start_urls100: {}'.format(self.start_urls[:100]))
        if userid < 0:
            self.start_urls = []
            # self.logger.info('---start_urls: {}'.format(self.start_urls))
        else:
            self.start_urls = self.start_urls[userid: userid + 1]
            # self.logger.info('---start_urls: {}'.format(self.start_urls))
            self.allowed_domains = [urlparse(self.start_urls[0]).netloc]
            # self.logger.info('---allowed_domains: {}'.format(self.allowed_domains))

        self.scraped_urls = scraped_links('{}_{}'.format(self.name, userid))
        # self.logger.info('---scraped_urls: {}'.format(self.scraped_urls))
        self.logger.info('Already scraped {} urls'.format(len(self.scraped_urls)))

        super(LjUserSpider, self).__init__(*args, **kwargs)

    def parse_item(self, response):
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

        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
