# -*- coding: utf-8 -*-
from scrapy import signals
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

# class WebcorpSpiderMiddleware(object):
#     # Not all methods need to be defined. If a method is not defined,
#     # scrapy acts as if the spider middleware does not modify the
#     # passed objects.
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         # This method is used by Scrapy to create your spiders.
#         s = cls()
#         crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
#         return s
#
#     def process_spider_input(self, response, spider):
#         # Called for each response that goes through the spider
#         # middleware and into the spider.
#
#         # Should return None or raise an exception.
#         return None
#
#     def process_spider_output(self, response, result, spider):
#         # Called with the results returned from the Spider, after
#         # it has processed the response.
#
#         # Must return an iterable of Request, dict or Item objects.
#         for i in result:
#             yield i
#
#     def process_spider_exception(self, response, exception, spider):
#         # Called when a spider or process_spider_input() method
#         # (from other spider middleware) raises an exception.
#
#         # Should return either None or an iterable of Request, dict
#         # or Item objects.
#         pass
#
#     def process_start_requests(self, start_requests, spider):
#         # Called with the start requests of the spider, and works
#         # similarly to the process_spider_output() method, except
#         # that it doesn’t have a response associated.
#
#         # Must return only requests (not items).
#         for r in start_requests:
#             yield r
#
#     def spider_opened(self, spider):
#         spider.logger.info('Spider opened: %s' % spider.name)
#
#
# class WebcorpDownloaderMiddleware(object):
#     # Not all methods need to be defined. If a method is not defined,
#     # scrapy acts as if the downloader middleware does not modify the
#     # passed objects.
#
#     @classmethod
#     def from_crawler(cls, crawler):
#         # This method is used by Scrapy to create your spiders.
#         s = cls()
#         crawler.signals.connect(s.spider_opened, signal=signals.spider_opened)
#         return s
#
#     def process_request(self, request, spider):
#         # Called for each request that goes through the downloader
#         # middleware.
#
#         # Must either:
#         # - return None: continue processing this request
#         # - or return a Response object
#         # - or return a Request object
#         # - or raise IgnoreRequest: process_exception() methods of
#         #   installed downloader middleware will be called
#         return None
#
#     def process_response(self, request, response, spider):
#         # Called with the response returned from the downloader.
#
#         # Must either;
#         # - return a Response object
#         # - return a Request object
#         # - or raise IgnoreRequest
#         return response
#
#     def process_exception(self, request, exception, spider):
#         # Called when a download handler or a process_request()
#         # (from other downloader middleware) raises an exception.
#
#         # Must either:
#         # - return None: continue processing this exception
#         # - return a Response object: stops process_exception() chain
#         # - return a Request object: stops process_exception() chain
#         pass
#
#     def spider_opened(self, spider):
#         spider.logger.info('Spider opened: %s' % spider.name)
