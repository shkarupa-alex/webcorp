# -*- coding: utf-8 -*-
from .sitemap import SitemapSpider


class SitemapSSpider(SitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': False,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        # 'CONCURRENT_REQUESTS_PER_IP': 1,
        # 'DOWNLOAD_DELAY': 4,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.67 Safari/537.36',
        'COOKIES_ENABLED': True
    }
    name = 'sitemaps'

    def start_requests(self):
        cook = 'is_scrollmode=1; pcid=SSbkazvq1v2; grex2=%7B%22reserve_user%22%3Atrue%7D; spua_c6bff6=%5B1%5D; gex1=%7B%22subs%22%3A%22f%22%2C%22thfds%22%3A%22f%22%2C%22frex3%22%3A%22f%22%7D; _ga=GA1.2.1094355687.1602327135; _fbp=fb.1.1602327140962.1416022766; _ym_uid=1585112905679078019; _ym_d=1602327141; __utma=43320484.1094355687.1602327135.1603696206.1603696206.1; __utmc=43320484; __utmz=43320484.1603696206.1.1.utmcsr=pikabu.ru|utmccn=(referral)|utmcmd=referral|utmcct=/; vn_buff=[7628359]; la=1607691271_750_1769_2124__; ulfs=1607691271; pkb_modern=11; k2s7=G-J:E:qZhC; bs=K1; _gid=GA1.2.1116284642.1607691273; _gat_gtag_UA_28292940_1=1; _ym_visorc_174977=b; _ym_isad=1; fps=d009aa30e8de1167ac6429d183c7acc4e8'
        cook = cook.split('; ')
        cook = [c.split('=') for c in cook]
        cook = {c[0]: c[1] for c in cook}

        for req in super(SitemapSSpider, self).start_requests():
            req.cookies = cook
            yield req
