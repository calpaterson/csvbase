from datetime import date
import pytest

from csvbase import exc
from csvbase.value_objs import Column, ColumnType
from csvbase.json import value_to_json, json_to_value

text_column = Column("some_text", ColumnType.TEXT)
integer_column = Column("an_int", ColumnType.INTEGER)
float_column = Column("a_float", ColumnType.FLOAT)
boolean_column = Column("a_boolean", ColumnType.BOOLEAN)
date_column = Column("a_date", ColumnType.DATE)


@pytest.mark.parametrize(
    "value, expected_json",
    [
        (date(2018, 1, 3), "2018-01-03"),
        (None, None),
    ],
)
def test_value_to_json(value, expected_json):
    actual_json = value_to_json(value)
    assert expected_json == actual_json


@pytest.mark.parametrize(
    "column, json_value, expected_value",
    [
        (text_column, "some text", "some text"),
        (integer_column, 1.0, 1.0),
        (integer_column, 1, 1),
        (float_column, 1.0, 1.0),
        (float_column, 1, 1.0),
        (boolean_column, True, True),
        (boolean_column, False, False),
        (date_column, "2018-01-03", date(2018, 1, 3)),
    ],
)
def test_json_to_value(column, json_value, expected_value):
    actual_value = json_to_value(column.type_, json_value)
    assert expected_value == actual_value


@pytest.mark.parametrize(
    "column", [text_column, integer_column, float_column, boolean_column, date_column]
)
def test_json_to_value_with_nulls(column):
    assert json_to_value(column.type_, None) is None


@pytest.mark.parametrize(
    "column, json_value",
    [
        (text_column, 1),
        (integer_column, "2018-01-02"),
        (boolean_column, "nope"),
        (date_column, "2018/01/03"),
    ],
)
def test_json_to_value_with_wrong_type(column, json_value):
    with pytest.raises(exc.UnconvertableValueException):
        json_to_value(column.type_, json_value)
