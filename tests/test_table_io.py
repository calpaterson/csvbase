import io
import csv
import pandas as pd
import string

import pytest

from csvbase.exc import CSVParseError
from csvbase.value_objs import Column, ColumnType
from csvbase import table_io
from csvbase.streams import rewind


def test_scientific_notation_not_put_into_csv():
    columns = [Column("a_float", type_=ColumnType.FLOAT)]
    rows = [[9.999999974e-07]]

    buf = table_io.rows_to_csv(columns, rows)
    csv_str = buf.getvalue()

    assert csv_str == b"a_float\r\n0.000001\r\n"


integer_col = Column("i", ColumnType.INTEGER)


@pytest.mark.parametrize(
    "csv_str, columns, expected_locations",
    [
        pytest.param(
            "i\na",
            [integer_col],
            [table_io.CSVParseErrorLocation(1, integer_col, "a")],
            id="text in int column",
        )
    ],
)
def test_csv_to_rows__errors(csv_str, columns, expected_locations):
    buf = io.StringIO(csv_str)
    with pytest.raises(CSVParseError) as e:
        x = list(table_io.csv_to_rows(buf, columns, csv.excel))
    assert e.value.error_locations == expected_locations


def test_csv_to_rows__many_errors():
    df = pd.DataFrame(dict(a=list(string.ascii_letters)))
    buf = io.StringIO()
    with rewind(buf):
        df.to_csv(buf, index=False)
    with pytest.raises(CSVParseError) as e:
        list(table_io.csv_to_rows(buf, [Column("a", ColumnType.INTEGER)], csv.excel))
