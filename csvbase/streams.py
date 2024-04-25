"""Functions and types for dealing with text and byte streams"""

import os
from logging import getLogger
from typing import Union, Tuple, Type, List, Dict, Set, IO, Optional, Sequence
import codecs
import csv
import io
import contextlib

from typing_extensions import Protocol
import charset_normalizer
import werkzeug

from .constants import COPY_BUFFER_SIZE
from . import conv, exc
from .value_objs import ColumnType, Column, Encoding

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


def detect_encoding(byte_buf: UserSubmittedBytes) -> Encoding:
    """Attempt to detect the encoding of the provided readable byte buffer.

    Falls back to utf-8 if unsuccessful.
    """
    with rewind(byte_buf):
        sample = byte_buf.read(COPY_BUFFER_SIZE)
        charset_matches = charset_normalizer.from_bytes(sample)
        bytes_read = byte_buf.tell()
    byte_count = file_length(byte_buf)

    match = charset_matches.best()
    if match is None:
        encoding = Encoding.UTF_8
        logger.warning("unable to detect encoding: falling back to utf-8")
    else:
        encoding = Encoding(match.encoding)
        logger.info("detected: %s after %d bytes", encoding, bytes_read)

    return encoding


def byte_buf_to_str_buf(
    byte_buf: UserSubmittedBytes, encoding: Optional[Encoding] = None
) -> codecs.StreamReader:
    """Convert a readable byte buffer into a readable str buffer.

    If no encoding is provided then an attempt is made to detect it."""
    encoding = encoding or detect_encoding(byte_buf)
    Reader = codecs.getreader(encoding.value)
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
    csv_buf: UserSubmittedCSVData, existing_columns: Optional[Sequence[Column]] = None
) -> Tuple[Type[csv.Dialect], List[Column]]:
    """Infer the csv dialect (usually: excel) and the column names/types) by
    looking at the top of it.

    If this is a csv for an existing table, the existing columns are provided
    and those are used instead of inferring..

    """
    # FIXME: this should be part of a more robust way to check the size of files
    with rewind(csv_buf):
        buf = csv_buf.read(COPY_BUFFER_SIZE)
        if len(buf) == 0 or buf.isspace():
            raise exc.CSVParseError("blank csv file")

    with rewind(csv_buf):
        dialect = sniff_csv(csv_buf)

        # FIXME: it's probably best that this consider the entire CSV file rather
        # than just the start of it.  there are many, many csv files that, halfway
        # down, switch out "YES" for "YES (but only <...>)"
        reader = csv.reader(csv_buf, dialect)
        headers = [
            header or f"col{i}" for i, header in enumerate(next(reader), start=1)
        ]

        # If we know what the types should be, don't infer, just return them
        # (in column order)
        if existing_columns is not None:
            existing_map = {
                existing_column.name: existing_column
                for existing_column in existing_columns
            }
            columns = []
            for header in headers:
                try:
                    columns.append(existing_map[header])
                except KeyError:
                    # a extra column is present
                    raise exc.TableDefinitionMismatchException()
            return dialect, columns

        first_few = zip(*(row for row, _ in zip(reader, range(1000))))
        as_dict: Dict[str, Set[str]] = dict(zip(headers, (set(v) for v in first_few)))

        cols = []
        ic = conv.IntegerConverter()
        dc = conv.DateConverter()
        fc = conv.FloatConverter()
        bc = conv.BooleanConverter()
        for key, values in as_dict.items():
            if key == "csvbase_row_id" or ic.sniff(values):
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


class Tellable(Protocol):
    def tell(self) -> int:
        pass


class TellableAndSeekable(Tellable, Seekable, Protocol):
    pass


class rewind:
    """Ensure that a stream is rewound after doing something.

    This is a common error and usually subtly messes up a sequence of
    operations on a file.

    By default, "seekbacks" are not allowed - this is the other common error,
    which is assume that you are at a certain point of the file which you are
    not actually at, which indicates a bug earlier in the code.
    """

    def __init__(
        self,
        stream: TellableAndSeekable,
        to: int = os.SEEK_SET,
        allow_seekback: bool = False,
    ) -> None:
        self.stream = stream
        self.to = to
        if self.stream.tell() != self.to and not allow_seekback:
            raise RuntimeError(
                "you not seeking back to where you are now - probably a bug"
            )

    def __enter__(self) -> None:
        pass

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        self.stream.seek(self.to)


def file_length(stream: TellableAndSeekable) -> int:
    with rewind(stream):
        stream.seek(0, os.SEEK_END)
        return stream.tell()
