import json
import csv
from typing import (
    List,
    Iterable,
    Mapping,
    Set,
    Tuple,
    Sequence,
    IO,
    Dict,
    Any,
    Optional,
)
from logging import getLogger
import io
from dataclasses import dataclass
import contextlib
import itertools

import xlsxwriter
import pyarrow as pa
import pyarrow.parquet as pq

from . import conv, exc
from .streams import UserSubmittedCSVData, rewind
from .value_objs import ColumnType, PythonType, Column, Table
from .json import value_to_json

logger = getLogger(__name__)

PARQUET_TYPE_MAP: Mapping[ColumnType, pa.lib.DataType] = {
    ColumnType.TEXT: pa.string(),
    ColumnType.INTEGER: pa.int64(),
    ColumnType.FLOAT: pa.float64(),
    ColumnType.BOOLEAN: pa.bool_(),
    ColumnType.DATE: pa.date32(),
}

PARQUET_READ_TYPE_MAP: Mapping[Tuple[str, str], ColumnType] = {
    ("INT64", "NONE"): ColumnType.INTEGER,
    ("BYTE_ARRAY", "STRING"): ColumnType.TEXT,
    ("INT32", "DATE"): ColumnType.DATE,
    ("DOUBLE", "NONE"): ColumnType.FLOAT,
    ("BOOLEAN", "NONE"): ColumnType.BOOLEAN,
}

UnmappedRow = Sequence[PythonType]


# Vendored from stdlib - use itertools.batched when on 3.12
def batched(iterable, n):
    # batched('ABCDEFG', 3) → ABC DEF G
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(itertools.islice(iterator, n)):
        yield batch


def rows_to_parquet(
    columns: Sequence[Column],
    rows: Iterable[UnmappedRow],
    buf: Optional[IO[bytes]] = None,
) -> IO[bytes]:
    # smaller is considerably better for csvbase as it keeps peak memory usage
    # down.  however there is evidence that very small numbers disportionately
    # slow down clients.  5k seems to be a just-about-workable midpoint.
    # https://duckdb.org/docs/guides/performance/file_formats#handling-parquet-files
    batch_size = 5_000
    buf = buf or io.BytesIO()

    # necessary to supply a schema in our case because pyarrow does not infer a
    # type for dates
    schema = pa.schema([pa.field(c.name, PARQUET_TYPE_MAP[c.type_]) for c in columns])

    column_names = [c.name for c in columns]
    with rewind(buf):
        with contextlib.closing(pq.ParquetWriter(buf, schema)) as writer:
            for batch in batched(rows, batch_size):
                pydict = {e[0]: pa.array(e[1]) for e in zip(column_names, zip(*batch))}
                record_batch = pa.RecordBatch.from_pydict(pydict, schema=schema)
                writer.write_batch(record_batch)
    return buf


@dataclass
class CSVParseErrorLocation:
    row: int
    column: Column
    value: str


def csv_to_rows(
    csv_buf: UserSubmittedCSVData,
    columns: Sequence[Column],
    dialect,
    error_threshold=10,
) -> Iterable[UnmappedRow]:
    """Parse a csv file into rows.

    If there are problems parsing the row values, CSVParseError will be raised
    with up to 10 error locations.

    """
    error_locations: List[CSVParseErrorLocation] = []
    reader = csv.reader(csv_buf, dialect)
    # FIXME: check that contents of this header matches the columns
    header = next(reader)  # pop the header, which is not useful
    logger.debug("header = '%s'", header)
    for index, line in enumerate(reader, start=1):
        row: List[PythonType] = []
        for column, cell in zip(columns, line):
            try:
                parsed_value = conv.from_string_to_python(column.type_, cell)
                row.append(parsed_value)
            except exc.UnconvertableValueException:
                error_locations.append(CSVParseErrorLocation(index, column, cell))
        error_count = len(error_locations)
        # stop yielding if we've encountered errors
        if error_count == 0:
            yield row
        elif error_count > error_threshold:
            break
    if error_locations:
        raise exc.CSVParseError("parse error(s)", error_locations)


def buf_to_pf(buf: IO[bytes]) -> pq.ParquetFile:
    return pq.ParquetFile(buf)


def parquet_file_to_columns(pf: pq.ParquetFile) -> List[Column]:
    columns = []
    for pf_column in pf.schema:
        type_str_pair: Tuple[str, str] = (
            pf_column.physical_type,
            pf_column.logical_type.type,
        )
        columns.append(Column(pf_column.name, PARQUET_READ_TYPE_MAP[type_str_pair]))
    return columns


def parquet_file_to_rows(pf: pq.ParquetFile) -> Iterable[UnmappedRow]:
    table = pf.read()
    for batch in table.to_batches():
        as_dict = batch.to_pydict()
        yield from zip(*as_dict.values())


def rows_to_csv(
    columns: Sequence[Column],
    rows: Iterable[UnmappedRow],
    delimiter: str = ",",
    buf: Optional[IO[bytes]] = None,
) -> IO[bytes]:
    # StringIOs are frustrating to return over http because you can't tell how
    # long they are (for the Content-Type header), so this follows the
    # pattern of the others in outputting to bytes
    buf = buf or io.BytesIO()
    with rewind(buf):
        text_buf = io.TextIOWrapper(buf)
        writer = csv.writer(text_buf, delimiter=delimiter)
        writer.writerow([col.name for col in columns])

        # This little section is to prevent scientific notation making it into the
        # csv.  Some csv parsers handle this but many choke.
        float_mask: Set[int] = {
            index
            for index, column in enumerate(columns)
            if column.type_ == ColumnType.FLOAT
        }
        rows_without_sci: Iterable[UnmappedRow] = (
            [
                (f"{cell:f}" if index in float_mask and cell is not None else cell)
                for index, cell in enumerate(row)
            ]
            for row in rows
        )

        writer.writerows(rows_without_sci)
        text_buf.detach()

    return buf


def make_xlsx_sheet_name(table: Table) -> str:
    """Turn a table name into an excel sheet name, obeying the various
    restrictions excel imposes on sheet names."""
    max_length = 31

    # slashes are not allowed, use semi-colon
    sheet_name = ";".join([table.username, table.table_name])

    # make it clear it's been abbreviated
    if len(sheet_name) > max_length:
        sheet_name = sheet_name[: max_length - 3] + "..."

    return sheet_name


def rows_to_xlsx(
    columns: Sequence[Column],
    rows: Iterable[UnmappedRow],
    sheet_name=None,
    excel_table: bool = False,
    buf: Optional[IO[bytes]] = None,
) -> IO[bytes]:
    column_names = [c.name for c in columns]

    # FIXME: Perhaps this should change based on the user's locale
    workbook_args: Dict = {"default_date_format": "yyyy-mm-dd"}
    if not excel_table:
        workbook_args["constant_memory"] = True

    buf = buf or io.BytesIO()
    with rewind(buf):
        with xlsxwriter.Workbook(buf, workbook_args) as workbook:
            worksheet = workbook.add_worksheet(name=sheet_name)

            if excel_table:
                rows = list(rows)
                table_args: Dict[str, Any] = {}
                table_args["data"] = rows
                table_args["columns"] = [
                    {"header": column_name} for column_name in column_names
                ]

                worksheet.add_table(
                    first_row=0,
                    first_col=0,
                    last_row=len(rows),
                    # FIXME: last_col should be zero indexed, isn't - bug?
                    last_col=len(columns) - 1,
                    options=table_args,
                )
            else:
                worksheet.write_row(0, 0, column_names)

                for index, row in enumerate(rows, start=1):
                    worksheet.write_row(index, 0, row)

    return buf


def rows_to_jsonlines(
    columns: Sequence[Column],
    rows: Iterable[UnmappedRow],
    buf: Optional[IO[bytes]] = None,
) -> IO[bytes]:
    buf = buf or io.BytesIO()

    column_names = [c.name for c in columns]
    with rewind(buf):
        text_buf = io.TextIOWrapper(buf)
        for row in rows:
            json.dump(
                dict(zip(column_names, (value_to_json(v) for v in row))), text_buf
            )
            text_buf.write("\n")
        text_buf.detach()
    return buf
