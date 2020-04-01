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
            if not page:
                self.src.task_done()
                return

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

        while True:
            page = self.trg.get()
            if not page:
                link_file.flush()
                link_file.close()
                text_file.flush()
                text_file.close()
                self.trg.task_done()
                return

            try:
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


def extract_drive2(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'itemprop': 'reviewBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'c-comment__text'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_dvach(html):
    soup = BeautifulSoup(html, 'lxml')

    comments = []
    for c in soup('article', {'class': 'post__message'}):
        for t in c.contents:
            if not isinstance(t, NavigableString):
                continue
            t = str(t).strip()
            if len(t):
                comments.append(t)

    return '\n\n\n'.join(comments)


def extract_habr(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'post__text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'comment__message'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


# def extract_kino(html):
#     soup = BeautifulSoup(html, 'lxml')
#
#     content = []
#
#     header = [str(node) for node in soup('h1')]
#     header = '<br><br><br>'.join(header)
#     content.append(header)
#
#     for node in soup('div', {'class': 'brand_words'}):
#         content.append(str(node))
#         content.append('<br>' * 10)
#
#     content = '<div>{}</div>'.format(''.join(content))
#
#     return fragment_to_text(content)


def extract_lenta(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_roem(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'comment-body'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_ria(html):
    soup = BeautifulSoup(html, 'lxml')

    for node in soup('div', {'class': 'm-image'}):
        node.extract()

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'article__body'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_vc(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'content--full'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'comments__item__text'}):
        content.append(str(node))
        content.append('<br>' * 3)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_gazeta(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_kommersant(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'article_text_wrapper'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_kp(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'ArticleDescription'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_mk(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'description'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_rbc(html):
    soup = BeautifulSoup(html, 'lxml')

    for node in soup('div', {'class': 'article__main-image'}):
        node.extract()

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'article__header__subtitle'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_sport(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'article_text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_woman(html):
    soup = BeautifulSoup(html, 'lxml')

    for node in soup('div', {'class': 'article-info'}):
        node.extract()

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'article__lead-paragraph'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'container__content-text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'card__comment'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_zen(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'articleBody'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'comment'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_otvet(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'itemprop': 'text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_pikabu(html):
    soup = BeautifulSoup(html, 'lxml')

    content = []

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)
    content.append(header)

    for node in soup('div', {'class': 'story__content'}):
        content.append(str(node))
        content.append('<br>' * 10)

    for node in soup('div', {'class': 'comment__content'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_lurk(html):
    if 'В базе данных не найдено' in html:
        return ''

    soup = BeautifulSoup(html, 'lxml')

    for node in soup('table', {'class': 'lm-plashka'}):
        node.extract()
    for node in soup('table', {'id': 'toc'}):
        node.extract()
    for node in soup('div', {'class': 'buttons-line'}):
        node.extract()
    for node in soup('div', {'class': 'noprint'}):
        node.extract()
    for node in soup(None, {'class': 'mw-collapsible'}):
        node.extract()

    content = []

    header = [str(node) for node in soup('h1')]

    header = '<br><br><br>'.join(header)
    content.append(header)

    for bad_title in [
        'User:', 'Mediawiki:', 'Special:', 'Lurkmore:', 'Участник:', 'Служебная:', 'Обсуждение:', 'Категория:',
        'Портал:', 'Обсуждение портала:', 'Шаблон:', 'Обсуждение участника:', 'Файл:', 'Обсуждение категории:',
        'Обсуждение шаблона:', 'Обсуждение копипасты:', 'Обсуждение смехуечков:', 'Обсуждение файла:',
        'Смехуечки:', 'Обсуждение MediaWiki:'
    ]:
        if bad_title in header:
            return ''

    for node in soup('div', {'id': 'mw-content-text'}):
        content.append(str(node))
        content.append('<br>' * 10)

    content = '<div>{}</div>'.format(''.join(content))

    return fragment_to_text(content)


def extract_article(html):
    if not len(html.strip()):
        return ''

    soup = BeautifulSoup(html, 'lxml')

    header = [str(node) for node in soup('h1')]
    header = '<br><br><br>'.join(header)

    article = html_to_article(html, 'ru')
    text = fragment_to_text('<div>' + header + '<br>' * 3 + article + '</div>')

    return text


def extract_text(url, html):
    if 'https://www.drive2.ru/' in url:
        return extract_drive2(html)

    if 'https://2ch.hk/' in url:
        return extract_dvach(html)

    if 'https://habr.com/' in url:
        return extract_habr(html)

    # if 'https://www.kinopoisk.ru/' in url:  # sitemap_0
    #     return extract_kino(html)

    if 'https://lenta.ru/' in url:  # sitemap_1
        return extract_lenta(html)

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

    if 'https://www.kp.ru/' in url:  # sitemap_7
        return extract_kp(html)

    if 'https://www.mk.ru/' in url:  # sitemap_8
        return extract_mk(html)

    if 'https://www.rbc.ru/' in url:  # sitemap_9
        return extract_rbc(html)

    if 'https://www.sport-express.ru/' in url:  # sitemap_10
        return extract_sport(html)

    if 'https://www.woman.ru/' in url:  # sitemap_11
        return extract_woman(html)

    if 'https://zen.yandex.ru/' in url:  # sitemap_12
        return extract_zen(html)

    # if 'https://irecommend.ru/' in url:  # sitemap_13
    #     return extract_irec(html)

    if 'https://otvet.mail.ru/' in url:  # sitemap_14
        if '/question/' not in url:
            return ''
        return extract_otvet(html)

    if 'https://pikabu.ru/' in url:  # sitemap_15
        return extract_pikabu(html)

    if 'http://lurkmore.net/' in url:
        return extract_lurk(html)

    return extract_article(html)


def csv_without_nulls(iterable):
    for line in iterable:
        if '\0' in line:
            print('NULL-byte found')
        yield line.replace('\0', '')


def _restart_workers(cnt, cur, src, trg):
    # Stop current workers
    for _ in cur:
        src.put(False)
    for wrk in cur:
        wrk.join()

    # Start next workers
    nxt = [Worker(src, trg) for _ in range(cnt)]
    for wrk in nxt:
        wrk.start()

    return nxt


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Extract text and links from csv.gz dump')
    parser.add_argument('src_file', type=argparse.FileType('rb'), help='source file')
    parser.add_argument('-workers', type=int, default=12, help='workers count')

    argv, _ = parser.parse_known_args()
    if argv.workers < 1:
        raise ValueError('Number of workers should be positive')

    src_name = argv.src_file.name
    argv.src_file.close()

    csv.field_size_limit(sys.maxsize)

    sources = JoinableQueue(argv.workers * 4)
    targets = JoinableQueue(argv.workers * 4)

    workers = _restart_workers(argv.workers, [], sources, targets)
    writer = Writer(src_name, targets)
    writer.start()

    progress = 0
    try:
        with gzip.open(src_name, 'rt', newline='', errors='replace') as f:
            reader = csv.DictReader(csv_without_nulls(f))

            try:
                for row in reader:
                    if len({'hash', 'url', 'html'}.difference(row.keys())):
                        raise KeyError('Wrong feed format')

                    if not row['hash'] or not row['url'] or not row['html']:
                        continue
                    if row['url'].startswith('http'):
                        sources.put(row)

                    progress += 1

                    if progress % 10000 == 0:
                        print('Processed {}0K files'.format(progress // 10000))

                    if progress % 100000 == 0:
                        print('Restarting workers')
                        workers = _restart_workers(argv.workers, workers, sources, targets)

            except csv.Error as e:
                print(e)

        workers = _restart_workers(0, workers, sources, targets)

    except KeyboardInterrupt:
        # Allow ^C to interrupt from any thread.
        print('Keyboard interrupt')

        try:
            while True: sources.get_nowait()
        except:
            pass
        sources.close()

        for wrk in workers:
            wrk.terminate()

    targets.put(False)
    writer.join()  # targets.join()
