# -*- coding: utf-8 -*-
import scrapy
from ..common import hash_row, scraped_links


class KpSpider(scrapy.Spider):
    custom_settings = {
        'FOLLOW_CANONICAL_LINKS': False,
        'ROBOTSTXT_OBEY': False,
        'REDIRECT_ENABLED': True,
    }
    name = 'kp'
    allowed_domains = [
        'www.alt.kp.ru', 'www.amur.kp.ru', 'www.astrakhan.kp.ru', 'www.bel.kp.ru', 'www.bryansk.kp.ru',
        'www.chel.kp.ru', 'www.chita.kp.ru', 'www.chukotka.kp.ru', 'www.crimea.kp.ru', 'www.cyprus.kp.ru',
        'www.donetsk.kp.ru', 'www.dv.kp.ru', 'www.eao.kp.ru', 'www.hab.kp.ru', 'www.irk.kp.ru', 'www.izh.kp.ru',
        'www.kaliningrad.kp.ru', 'www.kaluga.kp.ru', 'www.kamchatka.kp.ru', 'www.kazan.kp.ru', 'www.kem.kp.ru',
        'www.kirov.kp.ru', 'www.komi.kp.ru', 'www.kp.ru', 'www.krsk.kp.ru', 'www.kuban.kp.ru', 'www.kursk.kp.ru',
        'www.lipetsk.kp.ru', 'www.magadan.kp.ru', 'www.msk.kp.ru', 'www.murmansk.kp.ru', 'www.nnov.kp.ru',
        'www.nsk.kp.ru', 'www.omsk.kp.ru', 'www.orel.kp.ru', 'www.penza.kp.ru', 'www.perm.kp.ru', 'www.pskov.kp.ru',
        'www.rostov.kp.ru', 'www.ryazan.kp.ru', 'www.sakhalin.kp.ru', 'www.samara.kp.ru', 'www.saratov.kp.ru',
        'www.sevastopol.kp.ru', 'www.smol.kp.ru', 'www.spb.kp.ru', 'www.stav.kp.ru', 'www.tambov.kp.ru',
        'www.tomsk.kp.ru', 'www.tula.kp.ru', 'www.tumen.kp.ru', 'www.tver.kp.ru', 'www.ufa.kp.ru', 'www.ugra.kp.ru',
        'www.ul.kp.ru', 'www.ulan.kp.ru', 'www.ural.kp.ru', 'www.vladimir.kp.ru', 'www.volgograd.kp.ru',
        'www.vologda.kp.ru', 'www.vrn.kp.ru', 'www.yakutia.kp.ru', 'www.yamal.kp.ru', 'www.yar.kp.ru'
    ]
    start_urls = []

    url_template1 = 'https://www.kp.ru/daily/0/{}/'
    url_template2 = 'https://www.kp.ru/online/news/{}/'
    stop_post1 = 4218836  # 18.05.2020
    stop_post2 = 3874870  # 18.05.2020

    def __init__(self, *args, **kwargs):
        super(KpSpider, self).__init__(*args, **kwargs)

        scraped_urls = scraped_links(self.name)
        self.logger.info('Found {} scraped pages'.format(len(scraped_urls)))

        max1, max2 = 1, 1
        for url in scraped_urls:
            id = int(url.split('/')[-2])
            if '/daily/' in url:
                max1 = max(max1, id)
            if '/online/' in url:
                max2 = max(max2, id)

        for p in range(max1, self.stop_post1):
            url = self.url_template1.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

        for p in range(max2, self.stop_post2):
            url = self.url_template2.format(p)
            if url in scraped_urls:
                continue
            self.start_urls.append(url)

    def parse(self, response):
        yield {
            'hash': hash_row([response.url, response.text]),
            'url': response.url,
            'html': response.text
        }

    def response_is_ban(self, request, response):
        return False

    def exception_is_ban(self, request, exception):
        return None
