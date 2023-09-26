from pathlib import Path
from io import StringIO

import pytest

from csvbase import exc
from csvbase.value_objs import Column, ColumnType
from csvbase.streams import peek_csv, rewind

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
    "input_filename, expected_exception, expected_message",
    [("empty.csv", exc.CSVParseError, "blank csv")],
)
def test_peek_csv_with_junk(input_filename, expected_exception, expected_message):
    input_path = test_data / input_filename
    with input_path.open() as input_f:
        with pytest.raises(expected_exception) as e:
            peek_csv(input_f)
            assert e.msg == expected_message  # type: ignore


def test_rewind():
    buf = StringIO("hello")
    with rewind(buf):
        first_three = buf.read(3)
        assert first_three == "hel"
        assert buf.tell() == 3

    assert buf.tell() == 0
