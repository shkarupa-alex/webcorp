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
        cook = 'ab_var=6; _ga=GA1.2.840399676.1586104856; _gid=GA1.2.74964073.1586104856; _ym_uid=1586104856302145208; _ym_d=1586104856; stats_s_a=M%2B33yNyH65FI4UUiDBaFW4o%2F%2BXLTP747LGyFId0iwBhy9IMakAA1QJfbHwogVwBF6LXBpZgL9Y03VexdG2jtKL3OZwTTsmc%2BqfnC6up%2BT9jcC6vVWrlbmVvA97A6c8rv; ss_uid=15861048534187163; stats_u_a=7PsWe5Iq1pd9avMj9v1UDpCUD%2BnPCmGvcC2UzETJwHk9UzbvmXegiCk1%2BFgXOFotGh551vQNGm2sIiPFYBxRKDdKrOZB0%2BAoU%2BV232tE8hI%3D; _ym_isad=1; v=ed; _ym_visorc_8092381=b; ss_uid=15861048534187163; ss_hid=15273029; statsactivity=4; statstimer=3'
        cook = cook.split('; ')
        cook = [c.split('=') for c in cook]
        cook = {c[0]: c[1] for c in cook}

        for req in super(FSitemapSSpider, self).start_requests():
            req.cookies = cook
            yield req
