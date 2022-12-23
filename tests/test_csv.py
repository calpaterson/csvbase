from pathlib import Path

import pytest

from csvbase.value_objs import Column, ColumnType
from csvbase.svc import peek_csv

test_data = Path(__file__).resolve().parent / "test-data"

@pytest.mark.parametrize("input_filename, expected_columns", [
    ("abc123.csv", [Column("a", ColumnType.TEXT), Column("b", ColumnType.INTEGER), Column("c", ColumnType.FLOAT)])
])
def test_peek_csv(input_filename, expected_columns):
    input_path = test_data / input_filename
    with input_path.open() as input_f:
        _, actual_columns = peek_csv(input_f)

    assert actual_columns == expected_columns
