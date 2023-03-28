import csv
from typing import List, Iterable, Mapping, Collection, Tuple, Sequence, IO
import io

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

PARQUET_READ_TYPE_MAP: Mapping[str, ColumnType] = {
    "INT64": ColumnType.INTEGER,
    "STRING": ColumnType.TEXT,
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
    for column in pf.schema:
        if column.logical_type.type != "NONE":
            type_str = column.logical_type.type
        else:
            type_str = column.physical_type
        columns.append(Column(column.name, PARQUET_READ_TYPE_MAP[type_str]))
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
