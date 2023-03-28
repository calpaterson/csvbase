import csv
from typing import List, Iterable, Mapping, Collection, Tuple, Sequence, IO, Dict, Any
import io

import xlsxwriter
import pyarrow as pa
import pyarrow.parquet as pq

from . import conv
from .streams import UserSubmittedCSVData
from .value_objs import ColumnType, PythonType, Column

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

UnmappedRow = Collection[PythonType]


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
    pq.write_table(pa_table, parquet_buf)
    parquet_buf.seek(0)

    return parquet_buf


def csv_to_rows(
    csv_buf: UserSubmittedCSVData, columns: Sequence[Column], dialect
) -> Iterable[UnmappedRow]:
    reader = csv.reader(csv_buf, dialect)
    csv_buf.readline()  # pop the header, which is not useful
    row_gen = (
        [conv.from_string_to_python(col.type_, v) for col, v in zip(columns, line)]
        for line in reader
    )
    return row_gen


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
) -> io.StringIO:
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf, delimiter=delimiter)
    writer.writerow([col.name for col in columns])
    for row in rows:
        writer.writerow(row)
    csv_buf.seek(0)
    return csv_buf


def rows_to_xlsx(columns: Sequence[Column], rows: Iterable[UnmappedRow], excel_table: bool = False) -> io.BytesIO:
    xlsx_buf = io.BytesIO()

    column_names = [c.name for c in columns]

    # FIXME: Perhaps this should change based on the user's locale
    workbook_args: Dict = {"default_date_format": "yyyy-mm-dd"}
    if not excel_table:
        workbook_args["constant_memory"] = True

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

    xlsx_buf.seek(0)
    return xlsx_buf

