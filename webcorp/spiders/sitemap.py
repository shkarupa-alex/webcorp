# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class SitemapSpider(scrapy.spiders.SitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': False,
    }
    name = 'sitemap'
    sitemap_urls = [
        'https://www.kinopoisk.ru/robots.txt',  # 0 -> kino -> captcha
        'https://lenta.ru/robots.txt',  # 1
        'https://roem.ru/robots.txt',  # 2 done
        'https://ria.ru/robots.txt',  # 3
        'https://vc.ru/robots.txt',  # 4 done
        'https://www.gazeta.ru/robots.txt',  # 5
        'https://www.kommersant.ru/robots.txt',  # 6 -> kommersant
        'https://www.kp.ru/robots.txt',  # 7 -> kp
        'https://www.mk.ru/robots.txt',  # 8 todo check volg.*
        'https://www.rbc.ru/robots.txt',  # 9
        'https://www.sport-express.ru/robots.txt',  # 10
        'http://www.woman.ru/robots.txt',  # 11 todo: run with redirects
        'https://zen.yandex.ru/robots.txt',  # 12
        'https://irecommend.ru/robots.txt',  # 13
        'https://otvet.mail.ru/robots.txt',  # 14
        'https://pikabu.ru/robots.txt',  # 15 comments
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
