"""Functions and types for dealing with text and byte streams"""

import os
from logging import getLogger
from typing import Union, Tuple, Type, List, Dict, Set, IO, Optional
import codecs
import csv
import io
import contextlib

from typing_extensions import Protocol
from cchardet import UniversalDetector
import werkzeug

from . import conv, exc
from .value_objs import ColumnType, Column

# FIXME: This module needs a lot of work.  It should probably contain:
#
# 1. a set of typing protocols for readable/seekable/writable buffered and
#    unbufferred streams (aka filelikes)
# 2. a function for opening a (possibly unbuffered) byte stream of unknown
#    encoding as a _buffered_ text stream
#    - using charset-normalizer, not cchardet
# 3. a function for opening a text stream of unknown csv dialect as
#    Iterable[Row]

UserSubmittedCSVData = Union[codecs.StreamReader, io.StringIO]

UserSubmittedBytes = Union[werkzeug.datastructures.FileStorage, io.BytesIO]

logger = getLogger(__name__)


def byte_buf_to_str_buf(byte_buf: UserSubmittedBytes) -> codecs.StreamReader:
    """Convert a readable byte buffer into a readable str buffer.

    Tries to detect the character set along the way, falling back to utf-8."""
    detector = UniversalDetector()
    with rewind(byte_buf):
        for line in byte_buf.readlines():
            detector.feed(line)
            if detector.done:
                break
            if byte_buf.tell() > 1_000_000:
                logger.warning("unable to detect after 1mb, giving up")
                break
        logger.info("detected: %s after %d bytes", detector.result, byte_buf.tell())
    if detector.result["encoding"] is not None:
        encoding = detector.result["encoding"]
    else:
        logger.warning("unable to detect charset, assuming utf-8")
        encoding = "utf-8"
    Reader = codecs.getreader(encoding)
    return Reader(byte_buf)


def sniff_csv(
    csv_buf: UserSubmittedCSVData, sample_size_hint=8192
) -> Type[csv.Dialect]:
    """Return csv dialect and a boolean indicating a guess at whether there is
    a header."""
    sniffer = csv.Sniffer()

    with rewind(csv_buf):
        try:
            dialect = sniffer.sniff(csv_buf.read(sample_size_hint))
            logger.info("sniffed dialect: %s", dialect)
        except csv.Error:
            logger.warning("unable to sniff dialect, falling back to excel")
            dialect = csv.excel

    return dialect


def peek_csv(
    csv_buf: UserSubmittedCSVData,
) -> Tuple[Type[csv.Dialect], List[Column]]:
    """Infer the csv dialect (usually: excel) and the column names/types) by
    looking at the top of it.

    """
    # FIXME: this should be part of a more robust way to check the size of files
    with rewind(csv_buf):
        csv_buf.seek(0, os.SEEK_END)
        size = csv_buf.tell()
        if size == 0:
            raise exc.CSVException("empty file!")

    with rewind(csv_buf):
        dialect = sniff_csv(csv_buf)

        # FIXME: it's probably best that this consider the entire CSV file rather
        # than just the start of it.  there are many, many csv files that, halfway
        # down, switch out "YES" for "YES (but only <...>)"
        reader = csv.reader(csv_buf, dialect)
        headers = [
            header or f"col{i}" for i, header in enumerate(next(reader), start=1)
        ]
        first_few = zip(*(row for row, _ in zip(reader, range(1000))))
        as_dict: Dict[str, Set[str]] = dict(zip(headers, (set(v) for v in first_few)))
        cols = []
        ic = conv.IntegerConverter()
        dc = conv.DateConverter()
        fc = conv.FloatConverter()
        bc = conv.BooleanConverter()
        for key, values in as_dict.items():
            if ic.sniff(values):
                cols.append(Column(key, ColumnType.INTEGER))
            elif fc.sniff(values):
                cols.append(Column(key, ColumnType.FLOAT))
            elif bc.sniff(values):
                cols.append(Column(key, ColumnType.BOOLEAN))
            elif dc.sniff(values):
                cols.append(Column(key, ColumnType.DATE))
            else:
                cols.append(Column(key, ColumnType.TEXT))
        logger.info("inferred: %s", cols)

    return dialect, cols


class Seekable(Protocol):
    """A file that support seeking (don't care whether text or binary)."""

    def seek(self, offset: int, whence: int = 0) -> int:
        pass


class rewind:
    """Ensure that a stream is rewound after doing something.

    This is a common error and usually subtly messes up a sequence of
    operations eg reading from a csv (eg string encoding is broken, but
    delimiter detection is not).
    """

    def __init__(self, stream: Seekable) -> None:
        self.stream = stream

    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stream.seek(0)
