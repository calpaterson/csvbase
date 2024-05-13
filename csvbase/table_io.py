import json
import csv
from typing import List, Iterable, Mapping, Set, Tuple, Sequence, IO, Dict, Any
from logging import getLogger
import io
from dataclasses import dataclass

import xlsxwriter
import pyarrow as pa
import pyarrow.parquet as pq

from . import conv, exc
from .streams import UserSubmittedCSVData, rewind
from .value_objs import ColumnType, PythonType, Column
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


def rows_to_parquet(
    columns: Sequence[Column], rows: Iterable[UnmappedRow]
) -> io.BytesIO:
    # necessary to supply a schema in our case because pyarrow does not infer a
    # type for dates
    schema = pa.schema([pa.field(c.name, PARQUET_TYPE_MAP[c.type_]) for c in columns])

    column_names = [c.name for c in columns]
    mapping = [dict(zip(column_names, row)) for row in rows]

    pa_table = pa.Table.from_pylist(mapping, schema=schema)
    parquet_buf = io.BytesIO()
    with rewind(parquet_buf):
        pq.write_table(pa_table, parquet_buf)

    return parquet_buf


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
    columns: Sequence[Column], rows: Iterable[UnmappedRow], delimiter: str = ","
) -> io.BytesIO:
    # StringIOs are frustrating to return over http because you can't tell how
    # long they are (for the Content-Type header), so this follows the
    # pattern of the others in outputting to bytes
    csv_byte_buf = io.BytesIO()
    with rewind(csv_byte_buf):
        csv_buf = io.TextIOWrapper(csv_byte_buf)
        writer = csv.writer(csv_buf, delimiter=delimiter)
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
        csv_buf.detach()

    return csv_byte_buf


def rows_to_xlsx(
    columns: Sequence[Column], rows: Iterable[UnmappedRow], excel_table: bool = False
) -> io.BytesIO:
    column_names = [c.name for c in columns]

    # FIXME: Perhaps this should change based on the user's locale
    workbook_args: Dict = {"default_date_format": "yyyy-mm-dd"}
    if not excel_table:
        workbook_args["constant_memory"] = True

    xlsx_buf = io.BytesIO()
    with rewind(xlsx_buf):
        with xlsxwriter.Workbook(xlsx_buf, workbook_args) as workbook:
            worksheet = workbook.add_worksheet()

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

    return xlsx_buf


def rows_to_jsonlines(
    columns: Sequence[Column], rows: Iterable[UnmappedRow]
) -> io.BytesIO:
    jl_byte_buf = io.BytesIO()

    column_names = [c.name for c in columns]
    with rewind(jl_byte_buf):
        jl_buf = io.TextIOWrapper(jl_byte_buf)
        for row in rows:
            json.dump(dict(zip(column_names, (value_to_json(v) for v in row))), jl_buf)
            jl_buf.write("\n")
        jl_buf.detach()
    return jl_byte_buf
