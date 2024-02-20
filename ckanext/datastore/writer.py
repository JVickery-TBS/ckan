# encoding: utf-8
from __future__ import annotations

from io import StringIO, BytesIO

from typing import Any, Optional, Union
from simplejson import dumps

from xml.etree.cElementTree import Element, SubElement, ElementTree


class DatastoreDumpWriter(object):
    def __init__(self, output: Union[StringIO, BytesIO]):
        self.output = output

    def write_records(self, records: list[Any]) -> bytes:
        """
        Should write records to the output, seek, read,
        truncate, and return the output data as bytes.
        """
        raise NotImplementedError

    def end_file(self) -> bytes:
        """
        Should return any EOF characters as bytes.
        """
        raise NotImplementedError


class TextWriter(DatastoreDumpWriter):
    def write_records(self, records: list[Any]) -> bytes:
        self.output.write(records)  # type: ignore
        self.output.seek(0)
        output = self.output.read().encode('utf-8')
        self.output.truncate(0)
        self.output.seek(0)
        return output

    def end_file(self) -> bytes:
        return b''


class JSONWriter(DatastoreDumpWriter):
    def __init__(self, output: StringIO):
        super(JSONWriter, self).__init__(output)
        self.first = True

    def write_records(self, records: list[Any]) -> bytes:
        for r in records:
            if self.first:
                self.first = False
                self.output.write('\n    ')
            else:
                self.output.write(',\n    ')

            self.output.write(dumps(
                r, ensure_ascii=False, separators=(',', ':')))

        self.output.seek(0)
        output = self.output.read().encode('utf-8')
        self.output.truncate(0)
        self.output.seek(0)
        return output

    def end_file(self) -> bytes:
        return b'\n]}\n'


class XMLWriter(DatastoreDumpWriter):
    _key_attr = 'key'
    _value_tag = 'value'

    def __init__(self, output: BytesIO, columns: list[str]):
        super(JSONWriter, self).__init__(output)
        self.id_col = columns[0] == '_id'
        if self.id_col:
            columns = columns[1:]
        self.columns = columns

    def _insert_node(self, root: Any, k: str, v: Any,
                     key_attr: Optional[Any] = None):
        element = SubElement(root, k)
        if v is None:
            element.attrib['xsi:nil'] = 'true'
        elif not isinstance(v, (list, dict)):
            element.text = str(v)
        else:
            if isinstance(v, list):
                it = enumerate(v)
            else:
                it = v.items()
            for key, value in it:
                self._insert_node(element, self._value_tag, value, key)

        if key_attr is not None:
            element.attrib[self._key_attr] = str(key_attr)

    def write_records(self, records: list[Any]) -> bytes:
        for r in records:
            root = Element('row')
            if self.id_col:
                root.attrib['_id'] = str(r['_id'])
            for c in self.columns:
                self._insert_node(root, c, r[c])
            ElementTree(root).write(self.output, encoding='utf-8')
            self.output.write(b'\n')
        self.output.seek(0)
        output = self.output.read()
        self.output.truncate(0)
        self.output.seek(0)
        return output

    def end_file(self) -> bytes:
        return b'</data>\n'
