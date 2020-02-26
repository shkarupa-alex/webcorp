# -*- coding: utf-8 -*-
import scrapy
from scrapy.utils.project import get_project_settings
from ..storages import csv_joined_reader, check_row


class LurkSpider(scrapy.Spider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'TOR_PROXY_ENABLED': True,
        'ROBOTSTXT_OBEY': False,
        'AUTOTHROTTLE_ENABLED': False,
        'CONCURRENT_REQUESTS_PER_DOMAIN': 3,
        'CONCURRENT_REQUESTS_PER_IP': 3,
    }
    name = 'lurk'
    allowed_domains = [
        'lurkmore.to'
    ]
    start_urls = [
        # 'https://lurkmore.to/%D0%A1%D0%BB%D1%83%D0%B6%D0%B5%D0%B1%D0%BD%D0%B0%D1%8F:AllPages'
    ]

    csv_dump_name = 'lurkmore'
    url_template = 'http://lurkmore.to/index.php?oldid={}&printable=yes'
    stop_post = 3000000  # 05.02.2020

    def __init__(self, *args, **kwargs):
        super(LurkSpider, self).__init__(*args, **kwargs)

        max_id = 1
        scraped_urls = set()
        pool_path = get_project_settings().get('CSV_POOL_PATH')
        with csv_joined_reader(pool_path, self.csv_dump_name) as reader:
            for row in reader:
                if not check_row(row):
                    continue
                scraped_urls.add(row[1])
                try:
                    id = int(row[1].split('=')[1].split('&')[0])
                except:
                    id = -1
                if id < 0:
                    continue
                max_id = max(max_id, id)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        for p in range(max_id + 1, self.stop_post):
            url = self.url_template.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        yield {
            '__csv_dump_name': self.csv_dump_name,
            'url': response.url,
            'html': response.text
        }
