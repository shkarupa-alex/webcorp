# -*- coding: utf-8 -*-
import gzip
import os
import shutil
import tempfile
import unittest
from storages import WriteSplitter, CsvPooledWriter, csv_joined_reader


class WriteSplitterTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_wrong_files(self):
        with open(os.path.join(self.temp_dir, 'test-000000001.txt'), 'wt') as f:
            f.write('0' * 10)
        with open(os.path.join(self.temp_dir, 'wrong-test-2.txt'), 'wt') as f:
            f.write('1' * 10)
        with open(os.path.join(self.temp_dir, 'test-3.txt.wrong'), 'wt') as f:
            f.write('2' * 10)
        with open(os.path.join(self.temp_dir, 'test-4txt'), 'wt') as f:
            f.write('3' * 10)

        s = WriteSplitter(self.temp_dir, 'test', 'txt', max_size=1, compress=False)
        s.write(b'0' * 10)
        s.close()
        self.assertEqual(os.listdir(self.temp_dir), [
            'test-3.txt.wrong', 'test-4txt', 'wrong-test-2.txt', 'test-000000001.txt', 'test-000000002.txt'
        ])

    def test_no_compress(self):
        s = WriteSplitter(self.temp_dir, 'test', 'txt', max_size=10, compress=False)
        s.write(b'01234')
        s.write(b'-')
        s.write(b'56789')
        s.close()
        self.assertEqual(os.listdir(self.temp_dir), ['test-000000000.txt'])

        s = WriteSplitter(self.temp_dir, 'test', 'txt', max_size=10, compress=False)
        s.write(b'_____')
        s.close()
        self.assertEqual(os.listdir(self.temp_dir), ['test-000000001.txt', 'test-000000000.txt'])

        with open(os.path.join(self.temp_dir, 'test-000000000.txt'), 'rt') as f:
            content = f.read()
            self.assertEqual('01234-56789', content)

        with open(os.path.join(self.temp_dir, 'test-000000001.txt'), 'rt') as f:
            content = f.read()
            self.assertEqual('_____', content)

    def test_compress(self):
        s = WriteSplitter(self.temp_dir, 'test', 'txt', max_size=10)
        s.write(b'01234')
        s.write(b'-')
        s.write(b'56789')
        s.close()
        self.assertEqual(os.listdir(self.temp_dir), ['test-000000000.txt.gz'])

        s = WriteSplitter(self.temp_dir, 'test', 'txt', max_size=10)
        s.write(b'_____' * 2)
        s.write(b'-')
        s.write(b'_____' * 2)
        s.close()
        self.assertEqual(os.listdir(self.temp_dir), ['test-000000000.txt.gz', 'test-000000001.txt.gz'])

        with gzip.open(os.path.join(self.temp_dir, 'test-000000000.txt.gz'), 'rt') as f:
            content = f.read()
            self.assertEqual('01234-56789__________-', content)

        with gzip.open(os.path.join(self.temp_dir, 'test-000000001.txt.gz'), 'rt') as f:
            content = f.read()
            self.assertEqual('__________', content)


class CsvPooledWriterTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_rotate(self):
        c = CsvPooledWriter(pool_path=self.temp_dir, pool_size=2, file_size=5)
        c.write('first', ['01234', '56789'])
        self.assertEqual(c.order, ['first'])
        self.assertEqual(list(c.files.keys()), ['first'])
        self.assertEqual(os.listdir(self.temp_dir), ['first-000000000.csv.gz'])

        c.write('second', ['01234', '56789'])
        self.assertEqual(c.order, ['first', 'second'])
        self.assertEqual(list(c.files.keys()), ['first', 'second'])
        self.assertEqual(os.listdir(self.temp_dir), ['first-000000000.csv.gz', 'second-000000000.csv.gz'])

        c.write('first', ['01234', '56789'])
        self.assertEqual(c.order, ['second', 'first'])
        self.assertEqual(list(c.files.keys()), ['first', 'second'])
        self.assertEqual(os.listdir(self.temp_dir), [
            'first-000000000.csv.gz', 'second-000000000.csv.gz', 'first-000000001.csv.gz'
        ])

        c.write('third', ['01234', '56789'])
        self.assertEqual(c.order, ['first', 'third'])
        self.assertEqual(list(c.files.keys()), ['first', 'third'])
        self.assertEqual(os.listdir(self.temp_dir), [
            'third-000000000.csv.gz', 'first-000000000.csv.gz', 'second-000000000.csv.gz', 'first-000000001.csv.gz'
        ])


class CsvJoinedReaderTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_join_compressed(self):
        c = CsvPooledWriter(pool_path=self.temp_dir, pool_size=1, file_size=5)
        c.write('test', ['01\n234', '56789'])
        self.assertEqual(os.listdir(self.temp_dir), ['test-000000000.csv.gz'])

        c.write('test', ['ab"c"de', 'fg\r\nhij'])
        self.assertEqual(os.listdir(self.temp_dir), ['test-000000001.csv.gz', 'test-000000000.csv.gz'])

        c.close()

        with csv_joined_reader(self.temp_dir, 'test') as reader:
            data = list(reader)
            self.assertEqual(data, [
                ['01\n234', '56789'],
                ['ab"c"de', 'fg\r\nhij']
            ])


if __name__ == '__main__':
    unittest.main()
