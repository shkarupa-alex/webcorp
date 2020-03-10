#!/usr/bin/env python3
import argparse
import csv
import gzip
import hashlib
import sys
from bs4 import BeautifulSoup
from bs4.element import NavigableString
from nlpclean.html import html_to_article, fragment_to_text
from multiprocessing import JoinableQueue, Process


class Worker(Process):
    def __init__(self, source_queue, target_queue, *args, **kwargs):
        super(Worker, self).__init__(*args, **kwargs)
        self.src = source_queue
        self.trg = target_queue

    def run(self):
        print('Worker %d launched' % self.pid)

        while True:
            page = self.src.get()

            try:
                content = page['url'] + page['html']
                check = hashlib.md5(content.encode('utf-8')).hexdigest()

                if check == page['hash']:
                    text = extract_text(page['url'], page['html'])
                    text_content = page['url'] + text
                    text_check = hashlib.md5(text_content.encode('utf-8')).hexdigest()
                    link_check = hashlib.md5(page['url'].encode('utf-8')).hexdigest()

                    self.trg.put({
                        'empty': len(page['html'].strip()) == 0,
                        'link_hash': link_check,
                        'text_hash': text_check,
                        'url': page['url'],
                        'html': text,
                    })

            except Exception as e:
                print(e)

            self.src.task_done()


class Writer(Process):
    def __init__(self, source_name, target_queue, *args, **kwargs):
        super(Writer, self).__init__(*args, **kwargs)
        self.src = source_name
        self.trg = target_queue

    def run(self):
        print('Writer %d launched' % self.pid)

        link_file = gzip.open(self.src + '.link.csv.gz', 'wt', newline='')
        link_writer = csv.DictWriter(link_file, ['hash', 'url', 'html'])
        link_writer.writeheader()

        text_file = gzip.open(self.src + '.text.csv.gz', 'wt', newline='')
        text_writer = csv.DictWriter(text_file, ['hash', 'url', 'html'])
        text_writer.writeheader()

        link_duplicates = set()
        last_link = None
        last_text = None

        finished = False
        while not finished:
            page = self.trg.get()

            try:
                if 'finish' == page:
                    link_file.flush()
                    link_file.close()

                    text_file.flush()
                    text_file.close()
                    finished = True

                elif not finished:
                    if page['url'] in link_duplicates:
                        print('Duplicate link {}'.format(page['url']))
                        continue
                    else:
                        link_duplicates.add(page['url'])

                    if not page['empty'] and len(page['html']) and last_text == page['html']:
                        print('Duplicate text {} vs {}'.format(page['url'], last_link))
                        continue
                    elif not page['empty'] and len(page['html']):
                        last_text = page['html']
                        last_link = page['url']

                    link_writer.writerow({
                        'hash': page['link_hash'],
                        'url': page['url'],
                        'html': ''
                    })
                    if not page['empty']:
                        text_writer.writerow({
                            'hash': page['text_hash'],
                            'url': page['url'],
                            'html': page['html']
                        })

            except Exception as e:
                print('Error: {}'.format(e))

            self.trg.task_done()


# TODO: h1
def extract_habr(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'class': 'post__text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup.find_all('div', {'class': 'comment__message'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_dvach(html):
    soup = BeautifulSoup(html, 'lxml')

    comments = []
    for c in soup.find_all('article', {'class': 'post__message'}):
        for t in c.contents:
            if not isinstance(t, NavigableString):
                continue
            t = str(t).strip()
            if len(t):
                comments.append(t)

    return '\n\n\n'.join(comments)


def extract_kino(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'class': 'brand_words'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_lenta(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_roem(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup.find_all('div', {'class': 'comment-body'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_ria(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'class': 'article__body'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_vc(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'class': 'content--full'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup.find_all('div', {'class': 'comments__item__text'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_gazeta(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_kommersant(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup.find_all('div', {'class': 'article_text_wrapper'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_article(html):
    if not len(html.strip()):
        return ''

    soup = BeautifulSoup(html, 'lxml')

    header = [str(node) for node in soup.find_all('h1')]
    header = '<br><br><br>'.join(header)

    article = html_to_article(html, 'ru')
    text = fragment_to_text('<div>' + header + '<br>' * 3 + article + '</div>')

    return text


def extract_text(url, html):
    if 'https://habr.com/' in url:
        return extract_habr(html)

    if 'https://2ch.hk/' in url:
        return extract_dvach(html)

    if 'https://www.kinopoisk.ru/' in url:  # sitemap_0
        return extract_kino(html)

    if 'https://lenta.ru/' in url:  # sitemap_1
        return extract_lenta(html)

    if 'https://roem.ru/' in url:  # sitemap_2
        return extract_roem(html)

    if 'https://roem.ru/' in url:  # sitemap_2
        return extract_roem(html)

    if 'https://ria.ru/' in url:  # sitemap_3
        return extract_ria(html)

    if 'https://vc.ru/' in url:  # sitemap_4
        return extract_vc(html)

    if 'https://www.gazeta.ru/' in url:  # sitemap_5
        return extract_gazeta(html)

    if 'https://www.kommersant.ru/' in url:  # sitemap_6
        return extract_kommersant(html)

    return extract_article(html)


def csv_without_nulls(iterable):
    for line in iterable:
        if '\0' in line:
            print('NULL-byte found')
        yield line.replace('\0', '')


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract text and links from csv.gz dump')
    parser.add_argument('src_file', type=argparse.FileType('rb'), help='source file')
    parser.add_argument('-workers', type=int, default=8, help='workers count')

    argv, _ = parser.parse_known_args()
    if argv.workers < 1:
        raise ValueError('Number of workers should be positive')

    src_name = argv.src_file.name
    argv.src_file.close()

    csv.field_size_limit(sys.maxsize)

    sources = JoinableQueue(argv.workers * 4)
    targets = JoinableQueue(argv.workers * 4)
    workers = [Worker(sources, targets) for i in range(argv.workers)]
    writer = Writer(src_name, targets)

    for w in workers:
        w.daemon = True
        w.start()
    writer.start()

    progress = 0
    try:
        with gzip.open(src_name, 'rt', newline='') as f:
            reader = csv.DictReader(csv_without_nulls(f))

            try:
                for row in reader:
                    if len({'hash', 'url', 'html'}.difference(row.keys())):
                        raise KeyError('Wrong feed format')

                    if row['url'].startswith('http'):
                        sources.put(row)

                    progress += 1
                    if progress % 10000 == 0:
                        print('Processed {}0K files'.format(progress // 10000))
            except csv.Error as e:
                print(e)

        sources.join()

    except KeyboardInterrupt:
        # Allow ^C to interrupt from any thread.
        print('Keyboard interrupt')

    sources.close()
    try:
        while True: sources.get_nowait()
    except:
        pass

    for w in workers:
        w.terminate()

    targets.put('finish')
    writer.join()  # targets.join()
