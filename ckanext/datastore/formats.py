from contextlib import contextmanager
from typing import Any, Union
from io import StringIO, BytesIO
from codecs import BOM_UTF8
import csv
from json import dumps

from ckanext.datastore import writer

BOM = "\N{bom}"


class DatastoreDumpFormat(object):
    """
    Base class for pluggable Datastore Dump formats.
    """

    @property
    def ds_records_format(self) -> Union[str, None]:
        """
        Return None or a string: objects, lists, csv, or tsv
        """
        return None

    @property
    def file_extension(self) -> str:
        """
        Return a string for the file extension. Exclude dot (.)
        """
        raise NotImplementedError

    @property
    def content_type(self) -> str:
        """
        Return a string for the content type. E.g. "text/csv"
        """
        raise NotImplementedError

    @property
    def charset(self) -> str:
        """
        Return a string for the charset.
        """
        return 'utf-8'

    @property
    def bom(self) -> Union[bool, None]:
        """
        Return a boolean to write/use BOM in the file output.
        Or None to disable BOM.
        """
        return None

    @property
    def content_type_header(self) -> bytes:
        """
        Returns bytes to be used as the Content-Type header.
        """
        return b'%b; charset=%b' % (self.content_type.encode('utf-8'),
                                    self.charset.encode('utf-8'))

    def get_content_disposition_header(self, resource_id: str) -> str:
        """
        Returns a string to be used as the Content-disposition header.
        """
        return 'attachment; filename="%s.%s"' % (resource_id,
                                                 self.file_extension)

    @contextmanager
    def writer_factory(self, fields: list[dict[str, Any]]):
        """
        Return a generator that should write the Datastore fields to
        a StringIO or BytesIO object, then yield a DatastoreDumpWriter object.

        See: ckanext.datastore.writer.DatastoreDumpWriter
        """
        raise NotImplementedError

    def datastore_search_query(self, query_dict: dict[str, Any]):
        if not self.ds_records_format:
            raise NotImplementedError


class CSV(DatastoreDumpFormat):
    file_extension = 'csv'
    content_type = 'text/csv'
    bom = True
    ds_records_format = 'csv'

    @contextmanager
    def writer_factory(self, fields: list[dict[str, Any]]):
        output = StringIO()

        if self.bom:
            output.write(BOM)

        csv.writer(output).writerow(
            f['id'] for f in fields)
        yield writer.TextWriter(output)


class TSV(DatastoreDumpFormat):
    file_extension = 'tsv'
    content_type = 'text/tab-separated-values'
    bom = True
    ds_records_format = 'tsv'

    @contextmanager
    def writer_factory(self, fields: list[dict[str, Any]]):
        output = StringIO()

        if self.bom:
            output.write(BOM)

        csv.writer(
            output,
            dialect='excel-tab').writerow(
                f['id'] for f in fields)
        yield writer.TextWriter(output)


class JSON(DatastoreDumpFormat):
    file_extension = 'json'
    content_type = 'application/json'
    ds_records_format = 'lists'

    @contextmanager
    def writer_factory(self, fields: list[dict[str, Any]]):
        output = StringIO()

        output.write(
            '{\n  "fields": %s,\n  "records": [' % dumps(
                fields, ensure_ascii=False, separators=(',', ':')))
        yield writer.JSONWriter(output)


class XML(DatastoreDumpFormat):
    file_extension = 'xml'
    content_type = 'text/xml'
    ds_records_format = 'objects'

    @contextmanager
    def writer_factory(self, fields: list[dict[str, Any]]):
        output = StringIO()

        output.write(
            b'<data xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">\n')
        yield writer.XMLWriter(output, [f['id'] for f in fields])

