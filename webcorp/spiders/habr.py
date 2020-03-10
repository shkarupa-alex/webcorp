# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class HabrSpider(scrapy.Spider):
    name = 'habr'
    allowed_domains = ['habr.com']
    start_urls = [
        # 'https://habr.com/'
    ]

    url_template = 'https://habr.com/ru/post/{}/'
    stop_post = 491678  # 10.03.2020

    def __init__(self, *args, **kwargs):
        super(HabrSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        # max_id = 1
        # for url in scraped_urls:
        #     id = int(url.split('/')[-2])
        #     max_id = max(max_id, id)
        #
        # for p in range(max_id, self.stop_post):
        for p in range(1, self.stop_post):
            url = self.url_template.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        if '/en/' in response.url:
            self.logger.debug('Skip "en" url {}'.format(response.url))
            yield {
                'hash': hash_row([response.url, '']),
                'url': response.url,
                'html': ''
            }

        # comments = response.xpath('//*[contains(@class, "comment__message ")]')
        # comments = [c.extract().strip() for c in comments]
        # comments = [c for c in comments if len(c)]
        # comments = '\n\n\n'.join(comments)
        #
        # text = extract(cleanup(response.text)) + '\n' * 10 + comments

        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
