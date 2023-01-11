from pathlib import Path

import pytest

from csvbase import exc
from csvbase.value_objs import Column, ColumnType
from csvbase.streams import peek_csv, sniff_csv

test_data = Path(__file__).resolve().parent / "test-data"


@pytest.mark.parametrize(
    "input_filename, expected_columns",
    [
        (
            "abc123.csv",
            [
                Column("a", ColumnType.TEXT),
                Column("b", ColumnType.INTEGER),
                Column("c", ColumnType.FLOAT),
            ],
        ),
        (
            "blank-headers.csv",
            [
                Column("col1", ColumnType.INTEGER),
                Column("a", ColumnType.INTEGER),
                Column("col3", ColumnType.INTEGER),
                Column("b", ColumnType.INTEGER),
                Column("c", ColumnType.INTEGER),
                Column("col6", ColumnType.INTEGER),
            ],
        ),
    ],
)
def test_peek_csv(input_filename, expected_columns):
    input_path = test_data / input_filename
    with input_path.open() as input_f:
        _, actual_columns = peek_csv(input_f)

    assert actual_columns == expected_columns


@pytest.mark.parametrize(
    "input_filename, expected_exception",
    [("empty.csv", exc.CSVException)],
)
def test_sniff_csv_with_junk(input_filename, expected_exception):
    input_path = test_data / input_filename
    with input_path.open() as input_f:
        with pytest.raises(expected_exception):
            sniff_csv(input_f)
