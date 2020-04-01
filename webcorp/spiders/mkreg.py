# -*- coding: utf-8 -*-
import random
from scrapy.spiders import CrawlSpider, Rule
from scrapy.linkextractors import LinkExtractor
from ..common import hash_row, scraped_links


class MkRegSpider(CrawlSpider):
    custom_settings = {
        # 'FOLLOW_CANONICAL_LINKS': False,
    }
    name = 'mkreg'
    allowed_domains = []
    start_urls = []
    rules = (
        Rule(LinkExtractor(
            allow=(r'\/news\/\d+\/$',)
        )),
        Rule(
            LinkExtractor(allow=(r'\.html$',)),
            callback='parse_item'
        ),
    )

    start_domains = [
        'horizonnews.com.au', 'mkegypt.net', 'www.mklat.lv', 'arh.mk.ru', 'ast.mk.ru', 'baikal.mk.ru', 'brl.mk.ru',
        'cheb.mk.ru', 'chel.mk.ru', 'chr.mk.ru', 'chr.mk.ru', 'crimea.mk.ru', 'eburg.mk.ru', 'hab.mk.ru',
        'izhevsk.mk.ru', 'karel.mk.ru', 'kavkaz.mk.ru', 'kazan.mk.ru', 'kostroma.mk.ru', 'kras.mk.ru', 'kuban.mk.ru',
        'mk-kz.kz', 'mk-turkey.ru', 'mk.kn.md', 'mkala.mk.ru', 'mkisrael.co.il', 'murmansk.mk.ru', 'nn.mk.ru',
        'novos.mk.ru', 'omsk.mk.ru', 'oren.mk.ru', 'perm.mk.ru', 'rostov.mk.ru', 'rzn.mk.ru', 'saratov.mk.ru',
        'serp.mk.ru', 'spb.mk.ru', 'tambov.mk.ru', 'tomsk.mk.ru', 'tula.mk.ru', 'tumen.mk.ru', 'tver.mk.ru',
        'ufa.mk.ru', 'ugra.mk.ru', 'ulan.mk.ru', 'vladimir.mk.ru', 'volg.mk.ru', 'vologda.mk.ru', 'vrn.mk.ru',
        'www.mk-hakasia.ru', 'www.mk-kaliningrad.ru', 'www.mk-kalm.ru', 'www.mk-kamchatka.ru', 'www.mk-kirov.ru',
        'www.mk-kolyma.ru', 'www.mk-kuzbass.ru', 'www.mk-mari.ru', 'www.mk-pskov.ru', 'www.mk-sakhalin.ru',
        'www.mk-smolensk.ru', 'www.mk-tuva.ru', 'www.mk-yamal.ru', 'www.mk.kg', 'www.mk.ru', 'www.mkchita.ru',
        'www.mke.ee', 'www.mkivanovo.ru', 'www.mkkaluga.ru', 'www.mknews.de', 'www.vnovomsvete.com', 'yakutia.mk.ru',
        'yar.mk.ru'
    ]

    scraped_urls = set()

    def __init__(self, *args, **kwargs):
        for d in self.start_domains:
            self.allowed_domains.append(d)

            self.start_urls.append('https://{}/news/'.format(d))
            for y in ['2015', '2016', '2017', '2018', '2019', '2020']:
                for m in ['01', '02', '03', '04', '05', '06', '07', '08', '09', '10', '11', '12']:
                    for n in ['01', '02', '03', '04', '05', '06', '07', '08', '09'] + [str(i) for i in range(10, 32)]:
                        self.start_urls.append('https://{}/news/{}/{}/{}/'.format(d, y, m, n))
        random.shuffle(self.start_urls)

        self.scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(self.scraped_urls)))

        super(MkRegSpider, self).__init__(*args, **kwargs)

    def parse_item(self, response):
        if 'www.mk.ru' not in response.url:
            yield {
                'hash': hash_row([response.url, response.text]),
                'url': response.url,
                'html': response.text
            }
