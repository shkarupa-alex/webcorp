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
        'https://lenta.ru/robots.txt',  # 1 done
        'https://roem.ru/robots.txt',  # 2 done
        'https://ria.ru/robots.txt',  # 3 done
        'https://vc.ru/robots.txt',  # 4 done
        'https://www.gazeta.ru/robots.txt',  # 5 done
        'https://www.kommersant.ru/robots.txt',  # 6 (kommersant) done
        'https://www.kp.ru/robots.txt',  # 7 -> kp
        'https://www.mk.ru/robots.txt',  # 8 (+ mkreg) done
        'https://www.rbc.ru/robots.txt',  # 9 done
        'https://www.sport-express.ru/robots.txt',  # 10 (redirect) done
        'http://www.woman.ru/robots.txt',  # 11 (redirect)
        'https://zen.yandex.ru/robots.txt',  # 12
        'https://irecommend.ru/robots.txt',  # 13 todo: slowdown
        'https://otvet.mail.ru/robots.txt',  # 14
        'https://pikabu.ru/robots.txt',  # 15 (redirect)
    ]

    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        id = int(kwargs.pop('urlid', -1))
        if id < 0:
            self.sitemap_urls = []
        else:
            self.sitemap_urls = self.sitemap_urls[id: id + 1]

        self.scraped_urls = scraped_links('{}_{}'.format('sitemap', id))  # not sitemapr
        self.logger.info('Already scraped {} urls'.format(len(self.scraped_urls)))

        super(SitemapSpider, self).__init__(*args, **kwargs)

    def sitemap_filter(self, entries):
        for entry in entries:
            if 'loc' in entry:
                if '/tag/' in entry['loc'] or \
                        'otvet.mail.ru' in entry['loc'] and '/answer/' in entry['loc'] or \
                        'pikabu.ru' in entry['loc'] and '/story/' not in entry['loc'] or \
                        'irecommend.ru' in entry['loc'] and '/content/' not in entry['loc']:
                    continue
            yield entry

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
