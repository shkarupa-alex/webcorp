# -*- coding: utf-8 -*-
from .fsitemap import FSitemapSpider


class FSitemapSSpider(FSitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': False,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        'CONCURRENT_REQUESTS_PER_IP': 1,
        # 'DOWNLOAD_DELAY': 4,
        'USER_AGENT': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/86.0.4240.111 Safari/537.36',
        'COOKIES_ENABLED': True
    }
    ROTATING_PROXY_LIST = [
        'http://127.0.0.1:3128',
        'http://81.163.24.218:3128',  # iv
    ]
    name = 'fsitemaps'

    def start_requests(self):
        cook = 'is_scrollmode=1; pcid=SSbkazvq1v2; grex2=%7B%22reserve_user%22%3Atrue%7D; spua_c6bff6=%5B1%5D; gex1=%7B%22subs%22%3A%22f%22%2C%22thfds%22%3A%22f%22%2C%22frex3%22%3A%22f%22%7D; _ga=GA1.2.1094355687.1602327135; _fbp=fb.1.1602327140962.1416022766; _ym_uid=1585112905679078019; _ym_d=1602327141; d2mr6=A18549; __utma=43320484.1094355687.1602327135.1603696206.1603696206.1; __utmc=43320484; __utmz=43320484.1603696206.1.1.utmcsr=pikabu.ru|utmccn=(referral)|utmcmd=referral|utmcct=/; bs=J1; _gid=GA1.2.953331776.1604386050; _ym_visorc_174977=b; _ym_isad=1; vn_buff=[7628359]; la=1604386048_750_1206_1206__; ulfs=1604386152; pkb_modern=11; k2s7=G:C:RGhC; _gat_gtag_UA_12153200_2=1; _gat_gtag_UA_28292940_1=1; _ym_visorc_1959895=w'
        cook = cook.split('; ')
        cook = [c.split('=') for c in cook]
        cook = {c[0]: c[1] for c in cook}

        for req in super(FSitemapSSpider, self).start_requests():
            req.cookies = cook
            yield req
