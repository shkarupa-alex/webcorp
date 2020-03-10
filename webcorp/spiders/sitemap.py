# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class SitemapSpider(scrapy.spiders.SitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': False,
    }
    name = 'sitemap'
    sitemap_urls = [
        'https://www.kinopoisk.ru/robots.txt',  # kino
        'https://lenta.ru/robots.txt',  # ok
        'https://roem.ru/robots.txt',  # ok
        'https://ria.ru/robots.txt',  # in progress
        'https://vc.ru/robots.txt',  # 4 ok
        'https://www.gazeta.ru/robots.txt',  # in progress
        'https://www.kommersant.ru/robots.txt',  # kommersant
        'https://www.kp.ru/robots.txt',
        'https://www.mk.ru/robots.txt',
        'https://www.rbc.ru/robots.txt',
        'https://www.sport-express.ru/robots.txt',
        'http://www.woman.ru/robots.txt',
        'https://zen.yandex.ru/robots.txt',
        # 'https://irecommend.ru/robots.txt',  # custom parse
        # 'https://otvet.mail.ru/robots.txt',  # custom parse
        # 'https://pikabu.ru/robots.txt',  # custom parse comments
    ]

    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        id = int(kwargs.pop('urlid', -1))
        if id < 0:
            self.sitemap_urls = []
        else:
            self.sitemap_urls = self.sitemap_urls[id: id + 1]

        self.scraped_urls = scraped_links('{}_{}'.format(self.name, id))
        self.logger.info('Already scraped {} urls'.format(len(self.scraped_urls)))

        super(SitemapSpider, self).__init__(*args, **kwargs)

    def parse(self, response):
        # comments_roem = response.xpath('//*[contains(@class, "comment-body")]/p')
        # comments_vc = response.xpath('//*[contains(@class, "comments__item__text")]/p')
        # comments_woman = response.xpath('//*[contains(@itemprop, "comment")]//*[contains(@itemprop, "text")]')
        # comments = comments_roem +  comments_vc + comments_woman
        # comments = [c.extract() for c in comments]
        # comments = [c for c in comments if 'quote=' not in c]
        # comments = [c for c in comments if 'quote=' not in c]
        # comments = '\n\n\n'.join(comments)
        #
        # text = extract(cleanup(response.text)) + '\n' * 10 + comments

        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }
