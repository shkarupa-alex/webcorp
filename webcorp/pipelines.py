# -*- coding: utf-8 -*-
import os
from .storages import CsvPooledWriter


class WebcorpPipeline(object):
    def open_spider(self, spider):
        if not hasattr(spider, '_writer') or spider._writer is None:
            spider._writer = None

            pool_path = spider.settings.get('CSV_POOL_PATH', None)
            pool_subdir = spider.settings.get('CSV_POOL_SUBDIR', '')
            pool_size = spider.settings.get('CSV_POOL_SIZE', 1)
            max_size = spider.settings.get('CSV_FILE_SIZE', 0)
            compress = spider.settings.get('CSV_FILE_COMPRESS', False)

            if pool_path:
                spider._writer = CsvPooledWriter(
                    os.path.join(pool_path, pool_subdir),
                    pool_size,
                    max_size,
                    compress
                )

    def close_spider(self, spider):
        if spider._writer is not None:
            spider._writer.close()

    def process_item(self, item, spider):
        dump_name = item.pop('__csv_dump_name', False)
        if dump_name:
            values = dict(item).values()
            spider._writer.write(dump_name, values)

        return item
