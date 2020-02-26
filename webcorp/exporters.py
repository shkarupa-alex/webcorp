# -*- coding: utf-8 -*-
import gzip
from scrapy.exporters import CsvItemExporter


class GzipItemExporterMixin:
    def __init__(self, file, **kwargs):
        self.gzfile = gzip.GzipFile(fileobj=file)
        super(GzipItemExporterMixin, self).__init__(self.gzfile, **kwargs)

    def finish_exporting(self):
        super(GzipItemExporterMixin, self).finish_exporting()
        self.gzfile.close()


class CsvGzipItemExporter(GzipItemExporterMixin, CsvItemExporter):
    pass
