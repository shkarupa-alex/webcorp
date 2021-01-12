# -*- coding: utf-8 -*-
import bs4
import csv
import gzip
import hashlib
import os
import sys
from newspaper import fulltext
from scrapy.utils.project import get_project_settings


def hash_row(row):
    content = ''.join([str(v) for v in row])

    return hashlib.md5(content.encode('utf-8')).hexdigest()


def scraped_links(spider_name):
    csv.field_size_limit(sys.maxsize)

    result = set()

    storage_paths = get_project_settings().get('DEFAULT_EXPORT_STORAGES', [])
    for storage in storage_paths:
        if not os.path.exists(storage):
            continue

        for phase in ['', '.link.csv.gz'] + ['.link-{}.csv.gz'.format(i) for i in range(50)]:
            feed = os.path.join(storage, '{}.csv.gz{}'.format(spider_name, phase))
            if not os.path.exists(feed):
                continue

            with gzip.open(feed, 'rt', newline='') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if len({'hash', 'url', 'html'}.difference(row.keys())):
                        raise KeyError('Wrong feed format')

                    if hash_row([row['url'], row['html']]) == row['hash']:
                        result.add(row['url'])

    return result


def cleanup(html):
    soup = bs4.BeautifulSoup(html, 'lxml')

    for comment in soup.findAll(text=lambda node: isinstance(node, bs4.Comment)):
        comment.extract()
    for node in soup(['kbd', 'code', 'pre', 'samp', 'var', 'svg', 'script', 'style']):
        node.decompose()

    return str(soup)


def extract(html, language='ru'):
    try:
        text = fulltext(html=html, language=language)
    except:
        text = ''

    return text
