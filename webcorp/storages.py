# -*- coding: utf-8 -*-
import contextlib
import csv
import gzip
import hashlib
import io
import itertools
import os
import re
import sys


def hash_row(row):
    content = ''.join([str(v) for v in row])

    return hashlib.md5(content.encode('utf-8')).hexdigest()


def check_row(row):
    # Out of hash or values
    if len(row) < 2:
        return None

    # First value is not a hash
    hash_value = row[0]
    if not isinstance(hash_value, str) or len(hash_value) != 32 or set(hash_value).difference('0123456789abcdef'):
        return None

    return hash_row(row[1:]) == hash_value


class WriteSplitter:
    def __init__(self, file_path, file_name, file_ext, max_size=0, compress=True):
        """
        File-like object, that splits output to multiple files of a given size.

        Args:
            file_path: directory to store output files.
            file_name: base file name without extension.
            file_ext: file extension.
            max_size: maximum size of each file in bytes or `0` to disable splitting.
            compress: whether to write data with bzip compression.
        """
        self.file_path = file_path
        self.file_name = file_name
        self.file_ext = file_ext
        self.compress = compress
        self.max_size = max_size
        self.curr_file = None

    def __del__(self):
        self.close()

    def _next(self, force_next):
        exist_ext = '{}.gz'.format(self.file_ext) if self.compress else self.file_ext
        exist_pattern = '^' + re.escape(self.file_name) + r'-(\d+)\.' + re.escape(exist_ext) + '$'
        exist_matches = [re.match(exist_pattern, f) for f in os.listdir(self.file_path)]
        exist_indices = [int(m.group(1)) for m in exist_matches if m]

        last_index = max(exist_indices + [0])
        if force_next:
            last_index += 1
        last_index = str(last_index).zfill(9)

        return os.path.join(self.file_path, self.file_name + '-{}'.format(last_index) + '.{}'.format(self.file_ext))

    def _rotate(self):
        self._open(force_next=False)

        if self.max_size <= 0:
            return

        if self.curr_file.tell() > self.max_size:
            self._open(force_next=True)

    def _open(self, force_next):
        if force_next:
            self.close()

        if self.curr_file is not None:
            return

        open_fn = gzip.open if self.compress else open
        add_ext = '.gz' if self.compress else ''
        self.curr_file = open_fn(self._next(force_next) + add_ext, 'ab')

    def write(self, data):
        self._rotate()
        self.curr_file.write(data)

    def close(self):
        if self.curr_file is None:
            return

        self.curr_file.close()
        self.curr_file = None


class CsvPooledWriter:
    def __init__(self, pool_path, pool_size, max_size, compress=True):
        try:
            os.makedirs(pool_path)
        except IOError:
            pass
        if not (os.path.exists(pool_path) and os.path.isdir(pool_path)):
            raise IOError('Can\'t create path {}'.format(pool_path))

        self.pool_path = pool_path
        self.pool_size = pool_size
        self.max_size = max_size
        self.compress = compress

        self.buffer = io.StringIO()
        self.writer = csv.writer(self.buffer)

        self.order = []
        self.files = {}

    def __del__(self):
        self.close()

    def _encode(self, row):
        self.writer.writerow(row)
        result = self.buffer.getvalue()
        self.buffer.truncate(0)
        self.buffer.seek(0)

        return result.encode('utf-8')

    def write(self, name, row):
        if name not in self.order:
            self.files[name] = WriteSplitter(
                file_path=self.pool_path,
                file_name=name,
                file_ext='csv',
                max_size=self.max_size,
                compress=self.compress
            )
            self.order.append(name)
        else:
            self.order.remove(name)
            self.order.append(name)

        if len(self.order) > self.pool_size:
            last = self.order[0]
            self.files[last].close()
            del self.files[last]
            self.order.remove(last)

        hash = hash_row(row)
        data = self._encode([hash] + list(row))
        self.files[name].write(data)

    def close(self):
        for file in self.files.values():
            file.close()
        self.files = {}
        self.order = []


@contextlib.contextmanager
def csv_joined_reader(file_path, file_name, compressed=True):
    csv.field_size_limit(sys.maxsize)

    file_ext = 'csv\\.gz' if compressed else 'csv'
    exist_pattern = '^' + re.escape(file_name) + '-\\d+\\.{}$'.format(file_ext)
    exist_files = [f for f in os.listdir(file_path) if re.match(exist_pattern, f)]
    exist_files = [os.path.join(file_path, f) for f in sorted(exist_files)]

    open_fn = gzip.open if compressed else open
    opened_files = [open_fn(f, 'rt', newline='') for f in exist_files]
    csv_readers = [csv.reader(f) for f in opened_files]
    joined_reader = itertools.chain(*csv_readers)

    try:
        yield joined_reader
    finally:
        for f in opened_files:
            f.close()
