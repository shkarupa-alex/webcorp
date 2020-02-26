# -*- coding: utf-8 -*-
import scrapy
from scrapy.utils.project import get_project_settings
from ..common import cleanup, extract
from ..storages import csv_joined_reader, check_row


class HabrSpider(scrapy.Spider):
    name = 'habr'
    allowed_domains = ['habr.com']
    start_urls = [
        # 'https://habr.com/'
    ]

    csv_dump_name = 'habr'
    url_template = 'https://habr.com/ru/post/{}/'
    stop_post = 484118  # 15.01.2020

    def __init__(self, *args, **kwargs):
        super(HabrSpider, self).__init__(*args, **kwargs)

        max_id = 1
        scraped_urls = set()
        pool_path = get_project_settings().get('CSV_POOL_PATH')
        with csv_joined_reader(pool_path, self.csv_dump_name) as reader:
            for row in reader:
                if not check_row(row):
                    continue
                scraped_urls.add(row[1])
                id = int(row[1].split('/')[-2])
                max_id = max(max_id, id)

        for p in range(1, self.stop_post):
            url = self.url_template.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        if '/en/' in response.url:
            self.logger.debug('Skip "en" url {}'.format(response.url))
            return

        # comments = response.xpath('//*[contains(@class, "comment__message ")]')
        # comments = [c.extract().strip() for c in comments]
        # comments = [c for c in comments if len(c)]
        # comments = '\n\n\n'.join(comments)
        #
        # text = extract(cleanup(response.text)) + '\n' * 10 + comments
        text = response.text

        yield {
            '__csv_dump_name': self.csv_dump_name,
            'url': response.url,
            'html': text
        }
