# -*- coding: utf-8 -*-
from .fsitemap import FSitemapSpider


class FSitemapSSpider(FSitemapSpider):
    custom_settings = {
        'REDIRECT_ENABLED': False,
        # 'CONCURRENT_REQUESTS_PER_DOMAIN': 1,
        # 'CONCURRENT_REQUESTS_PER_IP': 1,
        # 'DOWNLOAD_DELAY': 4,
        'USER_AGENT': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.162 Safari/537.36',
        'COOKIES_ENABLED': True
    }
    name = 'fsitemaps'

    def start_requests(self):
        cook = 'is_scrollmode=1; pcid=4wEkIIVMFv2; spua_c6bff6=%5B0%5D; _ym_uid=1585112905679078019; _ym_d=1585112905; _ga=GA1.2.1087611889.1585112906; _fbp=fb.1.1585112905762.263246700; gex1=%7B%22thfds%22%3A%22f%22%2C%22frex%22%3A%22f%22%7D; _gid=GA1.2.1594755964.1589431579; _ym_isad=1; fps=d0091c09e00880932dd465d2fd4d6dbb14; vn=eJwzNjY0MzUHAAQ+ATo=; ycm=2; ulfs=1589433581; pkb_modern=11; bs=D1; _gat_gtag_UA_28292940_1=1; _ym_visorc_174977=b'
        cook = cook.split('; ')
        cook = [c.split('=') for c in cook]
        cook = {c[0]: c[1] for c in cook}

        for req in super(FSitemapSSpider, self).start_requests():
            req.cookies = cook
            yield req
