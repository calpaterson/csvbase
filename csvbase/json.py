from typing import Union, Optional
from datetime import date
import functools

from . import exc
from .value_objs import Column, PythonType, ColumnType

JsonType = Union[str, int, float, bool, None]


def value_to_json(value: Optional["PythonType"]) -> JsonType:
    if isinstance(value, date):
        return value.isoformat()
    else:
        return value


@functools.lru_cache(maxsize=128)
def json_to_value(
    column_type: ColumnType, json_value: JsonType
) -> Optional["PythonType"]:
    """Convert a 'json value' (ie: something returned from Python's json
    parser) into a value ready to be put into a Row."""
    if isinstance(json_value, str):
        if column_type is ColumnType.DATE:
            try:
                return date.fromisoformat(json_value)
            except ValueError as e:
                raise exc.UnconvertableValueException(column_type, json_value) from e
        elif column_type is ColumnType.TEXT:
            return json_value
        else:
            raise exc.UnconvertableValueException(column_type, json_value)
    elif isinstance(json_value, bool) and column_type is ColumnType.BOOLEAN:
        # NOTE: this must go ahead of the below case because in Python a bool
        # is an instance of int
        return json_value
    elif isinstance(json_value, (float, int)):
        if column_type is ColumnType.FLOAT:
            return float(json_value)
        elif column_type is ColumnType.INTEGER:
            return int(json_value)
        else:
            raise exc.UnconvertableValueException(column_type, str(json_value))
    elif json_value is None:
        return None
    else:
        # NOTE: this should be unreachable
        raise exc.UnconvertableValueException(column_type, str(json_value))
