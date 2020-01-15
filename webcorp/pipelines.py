# -*- coding: utf-8 -*-
import os
from .storages import CsvPooledWriter


class WebcorpPipeline(object):
    def open_spider(self, spider):
        pool_path = spider.settings.get('CSV_POOL_PATH')
        pool_subdir = spider.settings.get('CSV_POOL_SUBDIR')
        pool_size = spider.settings.get('CSV_POOL_SIZE')
        file_size = spider.settings.get('CSV_FILE_SIZE')
        self.writer = CsvPooledWriter(os.path.join(pool_path, pool_subdir), pool_size, file_size)

    def close_spider(self, spider):
        self.writer.close()

    def process_item(self, item, spider):
        dump_name = item.pop('__csv_dump_name', False)
        if dump_name:
            values = dict(item).values()
            self.writer.write(dump_name, values)

        return item
