#!/usr/bin/env python3
import argparse
import csv
import sys

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Repair CSV file')
    parser.add_argument('src_file', type=argparse.FileType('rt'), help='source file')
    parser.add_argument('trg_file', type=argparse.FileType('wt'), help='target file')

    argv, _ = parser.parse_known_args()

    src_name = argv.src_file.name
    argv.src_file.close()
    argv.src_file = open(src_name, 'rt', newline='')

    trg_name = argv.trg_file.name
    argv.trg_file.close()
    argv.trg_file = open(trg_name, 'wt', newline='')

    csv.field_size_limit(sys.maxsize)
    reader = iter(csv.reader(argv.src_file))
    writer = csv.writer(argv.trg_file)

    size = None
    while True:
        try:
            row = next(reader)

            if size is None:
                size = len(row)
            else:
                if len(row) != size:
                    raise AssertionError('Wrong row size')

            writer.writerow(row)
        except Exception as e:
            print(e)
            break

    argv.src_file.close()
    argv.trg_file.close()
