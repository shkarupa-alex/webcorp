#!/usr/bin/env python3
import argparse
import csv
import gzip
import hashlib
import sys
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
                    text = ''
                    if len(page['html']):
                        article = html_to_article(page['html'], 'ru')
                        text = fragment_to_text(article)

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
                    text_writer.writerow({
                        'hash': page['text_hash'],
                        'url': page['url'],
                        'html': page['html']
                    })

            except Exception as e:
                print('Error: {}'.format(e))

            self.trg.task_done()


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

    try:
        progress = 0
        with gzip.open(src_name, 'rt', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if len({'hash', 'url', 'html'}.difference(row.keys())):
                    raise KeyError('Wrong feed format')

                sources.put(row)

                progress += 1
                if progress % 100000 == 0:
                    print('Processed {}00K files'.format(progress // 100000))

        sources.join()
        targets.put('finish')
        targets.join()
        for w in workers:
            w.terminate()
        writer.join()

    except KeyboardInterrupt:
        # Allow ^C to interrupt from any thread.
        print('Keyboard interrupt')
        for w in workers:
            w.terminate()
        writer.terminate()

    except Exception as e:
        print(e)
        for w in workers:
            w.terminate()
        writer.terminate()
