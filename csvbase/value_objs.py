from typing import Optional, Sequence, Tuple, Union, Type
from typing_extensions import Literal
from uuid import UUID
from datetime import datetime, date
from dataclasses import dataclass
import enum

from sqlalchemy import types as satypes


@dataclass
class User:
    user_uuid: UUID
    username: str
    email: Optional[str]
    registered: datetime


@dataclass
class KeySet:
    """Used as a selector for keyset pagination

    https://use-the-index-luke.com/no-offset

    """

    n: int
    op: Literal["greater_than", "less_than"]
    size: int = 10


@dataclass
class Page:
    """A page from a table"""

    has_less: bool
    has_more: bool
    rows: Sequence[Tuple]


@enum.unique
class ColumnType(enum.Enum):
    TEXT = 1
    INTEGER = 2
    FLOAT = 3
    BOOLEAN = 4
    DATE = 5

    @staticmethod
    def from_sql_type(sqla_type: str) -> "ColumnType":
        return _REVERSE_SQL_TYPE_MAP[sqla_type]

    def sqla_type(self):
        return _SQLA_TYPE_MAP[self]

    def value_to_json(self, value) -> str:
        if self is ColumnType.DATE:
            return value.isoformat()
        else:
            return value

    def from_string_to_python(self, str) -> "PythonType":
        if self is ColumnType.DATE:
            return date.fromisoformat(str)
        else:
            return self.python_type()(str)

    def html_type(self) -> str:
        if self is ColumnType.BOOLEAN:
            return "checkbox"
        elif self is ColumnType.DATE:
            return "date"
        else:
            return "text"

    def pretty_name(self) -> str:
        return self.name.capitalize()

    def python_type(self) -> Type:
        return _PYTHON_TYPE_MAP[self]


PythonType = Union[int, bool, float, date, str]

_SQLA_TYPE_MAP = {
    ColumnType.TEXT: satypes.Text,
    ColumnType.INTEGER: satypes.BigInteger,
    ColumnType.FLOAT: satypes.Float,
    ColumnType.BOOLEAN: satypes.Boolean,
    ColumnType.DATE: satypes.Date,
}

_REVERSE_SQL_TYPE_MAP = {
    "BOOLEAN": ColumnType.BOOLEAN,
    "bigint": ColumnType.INTEGER,
    "date": ColumnType.DATE,
    "double precision": ColumnType.FLOAT,
    "integer": ColumnType.INTEGER,
    "text": ColumnType.TEXT,
}

_PYTHON_TYPE_MAP = {
    ColumnType.TEXT: str,
    ColumnType.INTEGER: int,
    ColumnType.FLOAT: float,
    ColumnType.BOOLEAN: bool,
    ColumnType.DATE: date,
}


@dataclass(frozen=True)
class Column:
    name: str
    type_: ColumnType
