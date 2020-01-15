# -*- coding: utf-8 -*-
import scrapy
from scrapy.utils.project import get_project_settings
from ..storages import csv_joined_reader


class HabrSpider(scrapy.Spider):
    name = 'habr'
    allowed_domains = ['habr.com']
    start_urls = [
        # 'https://habr.com/'
    ]

    csv_dump_name = 'habr'
    stop_post = 484018 - 10000  # 31.10.2019
    url_template = 'https://habr.com/ru/post/{}/'

    def __init__(self, *args, **kwargs):
        super(HabrSpider, self).__init__(*args, **kwargs)

        start_post = 1
        pool_path = get_project_settings().get('CSV_POOL_PATH')
        with csv_joined_reader(pool_path, self.csv_dump_name) as reader:
            for row in reader:
                start_post = max(start_post, int(row[0]))

        for p in range(start_post, self.stop_post):
            self.start_urls.append(self.url_template.format(p))

    def parse(self, response):
        if '/en/' in response.url:
            self.logger.debug('Skip "en" url {}'.format(response.url))
            return

        id = int(response.url.split('/')[-2])
        if id < 1:
            self.logger.warning('Wrong "id" in url {}'.format(response.url))
            return

        html = response.body.decode(response.encoding)

        yield {
            '__csv_dump_name': self.csv_dump_name,
            'id': id,
            'html': html
        }
