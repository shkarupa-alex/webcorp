# -*- coding: utf-8 -*-
from scrapy.downloadermiddlewares.redirect import MetaRefreshMiddleware
from scrapy.linkextractors import LinkExtractor
from scrapy.http import HtmlResponse, Request
from scrapy.utils.project import get_project_settings
from stem import Signal
from stem.control import Controller
from w3lib.url import safe_url_string


class AlreadyScrapedSpiderMiddleware(object):
    @classmethod
    def from_crawler(cls, crawler):
        return cls()

    def process_spider_output(self, response, result, spider):
        enabled = hasattr(spider, 'scraped_urls') and isinstance(spider.scraped_urls, set)

        for r in result:
            if enabled and isinstance(r, Request):
                r_htt1 = r.url.replace('http:', 'https:')
                r_htt2 = r.url.replace('https:', 'http:')
                if r_htt1 in spider.scraped_urls or r_htt2 in spider.scraped_urls:
                    spider.logger.debug('Skip already scraped url {}'.format(r.url))
                    continue

            yield r


class RelCanonicalDownloaderMiddleware(MetaRefreshMiddleware):
    extractor = LinkExtractor(
        restrict_xpaths=['//head/link[@rel="canonical"]'],
        tags=['link'],
        attrs=['href']
    )

    def process_response(self, request, response, spider):
        if request.method == 'HEAD' or not isinstance(response, HtmlResponse):
            return response

        if not spider.settings.get('FOLLOW_CANONICAL_LINKS', False) or not response.body:
            return response

        rel_canonical = self.extractor.extract_links(response)
        if not len(rel_canonical):
            return response

        rel_canonical = rel_canonical[0].url
        if rel_canonical == request.url or rel_canonical == response.url:
            return response

        redirected_url = safe_url_string(rel_canonical)
        redirected = self._redirect_request_using_get(request, redirected_url)

        return self._redirect(redirected, request, spider, 'rel canonical')


class TorProxyMiddleware(object):
    def __init__(self):
        self.req_counter = 0

        settings = get_project_settings()
        self.password = settings.get('TOR_AUTH_PASSWORD', '')
        self.http_proxy = settings.get('TOR_HTTP_PROXY', None)
        self.control_port = settings.get('TOR_CONTROL_PORT', 9051)
        self.max_req_per_ip = settings.get('TOR_MAX_REQ_PER_IP', None)
        self.exit_nodes = settings.get('TOR_EXIT_NODES', None)
        if self.exit_nodes:
            with Controller.from_port(port=self.control_port) as controller:
                controller.authenticate(self.password)
                controller.set_conf('ExitNodes', self.exit_nodes)
                controller.close()

    def change_ip_address(self):
        with Controller.from_port(port=self.control_port) as controller:
            controller.authenticate(self.password)
            controller.signal(Signal.NEWNYM)
            controller.close()

    def process_request(self, request, spider):
        if not spider.settings.get('TOR_PROXY_ENABLED', False):
            return None

        self.req_counter += 1
        if self.max_req_per_ip is not None and self.req_counter > self.max_req_per_ip:
            self.req_counter = 0
            self.change_ip_address()

        request.meta['proxy'] = self.http_proxy
        spider.logger.debug('Using proxy: {} for {}'.format(request.meta['proxy'], request.url))

        return None
