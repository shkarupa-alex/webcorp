# -*- coding: utf-8 -*-
import os
import logging
import sys

# Scrapy settings for webcorp project
#
# For simplicity, this file contains only settings considered important or
# commonly used. You can find more settings consulting the documentation:
#
#     https://docs.scrapy.org/en/latest/topics/settings.html
#     https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
#     https://docs.scrapy.org/en/latest/topics/spider-middleware.html

BOT_NAME = 'webcorp'

SPIDER_MODULES = ['webcorp.spiders']
NEWSPIDER_MODULE = 'webcorp.spiders'

# Crawl responsibly by identifying yourself (and your website) on the user-agent
USER_AGENT = 'Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)'

# Obey robots.txt rules
ROBOTSTXT_OBEY = True

# Configure maximum concurrent requests performed by Scrapy (default: 16)
# CONCURRENT_REQUESTS = 32

# Configure a delay for requests for the same website (default: 0)
# See https://docs.scrapy.org/en/latest/topics/settings.html#download-delay
# See also autothrottle settings and docs
# DOWNLOAD_DELAY = 3
# The download delay setting will honor only one of:
CONCURRENT_REQUESTS_PER_DOMAIN = 2  # 16
CONCURRENT_REQUESTS_PER_IP = 2  # 16

# Disable cookies (enabled by default)
COOKIES_ENABLED = False

# Disable Telnet Console (enabled by default)
# TELNETCONSOLE_ENABLED = False

# Override the default request headers:
# DEFAULT_REQUEST_HEADERS = {
#   'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#   'Accept-Language': 'en',
# }

# Enable or disable spider middlewares
# See https://docs.scrapy.org/en/latest/topics/spider-middleware.html
SPIDER_MIDDLEWARES = {
    'webcorp.middlewares.AlreadyScrapedSpiderMiddleware': 543
}

# Enable or disable downloader middlewares
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html
DOWNLOADER_MIDDLEWARES = {
    'webcorp.middlewares.TorProxyMiddleware': 410,
    'webcorp.middlewares.RelCanonicalDownloaderMiddleware': 585,
    'rotating_proxies.middlewares.RotatingProxyMiddleware': 610,
    'rotating_proxies.middlewares.BanDetectionMiddleware': 620,
}

# Enable or disable extensions
# See https://docs.scrapy.org/en/latest/topics/extensions.html
EXTENSIONS = {
    # 'scrapy.extensions.telnet.TelnetConsole': None,
}

# Configure item pipelines
# See https://docs.scrapy.org/en/latest/topics/item-pipeline.html
# ITEM_PIPELINES = {}

# Enable and configure the AutoThrottle extension (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/autothrottle.html
# AUTOTHROTTLE_ENABLED = True
# The initial download delay
# AUTOTHROTTLE_START_DELAY = 5
# The maximum download delay to be set in case of high latencies
# AUTOTHROTTLE_MAX_DELAY = 60
# The average number of requests Scrapy should be sending in parallel to each remote server
# AUTOTHROTTLE_TARGET_CONCURRENCY = 1.0
# Enable showing throttling stats for every response received:
# AUTOTHROTTLE_DEBUG = False

# Enable and configure HTTP caching (disabled by default)
# See https://docs.scrapy.org/en/latest/topics/downloader-middleware.html#httpcache-middleware-settings
# HTTPCACHE_ENABLED = True
# HTTPCACHE_EXPIRATION_SECS = 0
# HTTPCACHE_DIR = 'httpcache'
# HTTPCACHE_IGNORE_HTTP_CODES = []
# HTTPCACHE_STORAGE = 'scrapy.extensions.httpcache.FilesystemCacheStorage'

# Custom settings
LOG_LEVEL = logging.INFO

ROBOTSTXT_USER_AGENT = 'GoogleBot'

AUTOTHROTTLE_ENABLED = False

FOLLOW_CANONICAL_LINKS = True

ROTATING_PROXY_LIST = None
ROTATING_PROXY_LOGSTATS_INTERVAL = 300

TOR_PROXY_ENABLED = False
TOR_HTTP_PROXY = 'http://127.0.0.1:8118'
TOR_AUTH_PASSWORD = 'secretPassword'
TOR_CONTROL_PORT = 9051
TOR_MAX_REQ_PER_IP = 500

FEED_EXPORTERS = {
    'csv.gz': 'webcorp.exporters.CsvGzItemExporter',
}
FEED_FORMAT = 'csv.gz'

PRODUCTION_EXPORT_STORAGE = os.path.join('/', 'mnt', 'HDD', 'export')
DEFAULT_EXPORT_STORAGES = [
    PRODUCTION_EXPORT_STORAGE,
    os.path.join(os.path.dirname(__file__), '..', 'export')
]

if os.path.exists(PRODUCTION_EXPORT_STORAGE):
    ROTATING_PROXY_LIST = [
        'http://127.0.0.1:3128',
        # 'http://94.130.10.45:3128', # ivdev
        'http://95.216.99.233:3128',
        'http://95.217.76.210:3128',
        'http://81.163.24.218:3128',  # iv
    ]

# HTTPCACHE_ENABLED = True
# HTTPCACHE_DIR = './cache'
