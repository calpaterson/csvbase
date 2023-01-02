"""Functions and types for dealing with text and byte streams"""

from logging import getLogger
from typing import Union, Tuple, Type, List, Dict, Set
import codecs
import csv
import io

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
    for line in byte_buf.readlines():
        detector.feed(line)
        if detector.done:
            break
        if byte_buf.tell() > 1_000_000:
            logger.warning("unable to detect after 1mb, giving up")
            break
    logger.info("detected: %s after %d bytes", detector.result, byte_buf.tell())
    byte_buf.seek(0)
    if detector.result["encoding"] is not None:
        encoding = detector.result["encoding"]
    else:
        logger.warning("unable to detect charset, assuming utf-8")
        encoding = "utf-8"
    Reader = codecs.getreader(encoding)
    return Reader(byte_buf)


def sniff_csv(
    csv_buf: UserSubmittedCSVData, sample_size_hint: int = 2**13
) -> Tuple[Type[csv.Dialect], bool]:
    """Return csv dialect and a boolean indicating a guess at whether there is
    a header."""
    sniffer = csv.Sniffer()
    sample = "".join(csv_buf.readlines(sample_size_hint))

    try:
        dialect = sniffer.sniff(sample)
    except csv.Error:
        logger.warning("unable to sniff dialect, falling back to excel")
        dialect = csv.excel
    logger.info("sniffed dialect: %s", dialect)

    try:
        has_header = sniffer.has_header(sample)
    except csv.Error as e:
        logger.warning("unable to parse header, original args: %s", e.args)
        raise exc.CSVException("unable to parse header")
    logger.info("has_header = %s", has_header)

    csv_buf.seek(0)

    return dialect, has_header


def peek_csv(
    csv_buf: UserSubmittedCSVData,
) -> Tuple[Type[csv.Dialect], List[Column]]:
    """Infer the csv dialect (usually: excel) and the column names/types) by
    looking at the top of it.

    """
    dialect, _ = sniff_csv(csv_buf)

    # FIXME: it's probably best that this consider the entire CSV file rather
    # than just the start of it.  there are many, many csv files that, halfway
    # down, switch out "YES" for "YES (but only <...>)"
    reader = csv.reader(csv_buf, dialect)
    headers = next(reader)
    first_few = zip(*(row for row, _ in zip(reader, range(1000))))
    as_dict: Dict[str, Set[str]] = dict(zip(headers, (set(v) for v in first_few)))
    cols = []
    # Don't try to infer ints here, just too hard to tell them apart from floats
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
