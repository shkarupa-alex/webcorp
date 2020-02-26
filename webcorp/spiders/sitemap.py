# -*- coding: utf-8 -*-
import scrapy
from scrapy.utils.project import get_project_settings
from six.moves.urllib.parse import urlparse
from slugify import slugify  # python-slugify
from ..common import cleanup, extract
from ..storages import csv_joined_reader, check_row


class SitemapSpider(scrapy.spiders.SitemapSpider):
    name = 'sitemap'
    sitemap_urls = [
        'https://www.kinopoisk.ru/robots.txt',
        'https://lenta.ru/robots.txt', # 1 todo
        'https://roem.ru/robots.txt', # 2 ok
        'https://ria.ru/robots.txt',
        'https://vc.ru/robots.txt', # 4 ok
        'https://www.gazeta.ru/robots.txt',
        'https://www.kommersant.ru/robots.txt',
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
        try:
            id_g = int(get_project_settings().get('SITEMAP_SPIDER_ID', -1))
        except:
            id_g = -1
        try:
            id_l = int(self.settings.get('SITEMAP_SPIDER_ID', -1))
        except:
            id_l = -1
        id = max(id_l, id_g)
        id = int(kwargs.pop('urlid', id))
        if id < 0:
            self.sitemap_urls = []
        else:
            self.sitemap_urls = self.sitemap_urls[id: id + 1]
        self.logger.info('sitemap_urls:')
        self.logger.info(self.sitemap_urls)

        super(SitemapSpider, self).__init__(*args, **kwargs)

        pool_path = get_project_settings().get('CSV_POOL_PATH')
        for start_url in self.sitemap_urls:
            dump_name = SitemapSpider.csv_dump_name(start_url)
            with csv_joined_reader(pool_path, dump_name) as reader:
                for row in reader:
                    if not check_row(row):
                        continue
                    self.scraped_urls.add(row[1])

    @staticmethod
    def csv_dump_name(url):
        domain = urlparse(url).netloc
        slug = slugify(domain)

        return slug

    def parse(self, response):
        dump_name = SitemapSpider.csv_dump_name(response.url)

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
        text = response.text

        yield {
            '__csv_dump_name': dump_name,
            'url': response.url,
            'html': text
        }
