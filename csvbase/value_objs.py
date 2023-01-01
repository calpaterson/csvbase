import io
import codecs
from typing import (
    Optional,
    Sequence,
    Type,
    Iterable,
    Any,
    Dict,
    Union,
    Mapping,
)
from typing_extensions import Literal
from uuid import UUID
from datetime import datetime, date, timedelta, timezone
from dataclasses import dataclass
import enum
import binascii

from sqlalchemy import types as satypes
import werkzeug.datastructures

UserSubmittedCSVData = Union[codecs.StreamReader, io.StringIO]

UserSubmittedBytes = Union[werkzeug.datastructures.FileStorage, io.BytesIO]

# Preliminary version of a Row.  Another option would be to subclass tuple and
# implement __getattr__ to provide access by column
Row = Mapping["Column", Optional["PythonType"]]


@dataclass
class User:
    user_uuid: UUID
    username: str
    email: Optional[str]
    registered: datetime
    api_key: bytes

    def hex_api_key(self) -> str:
        return binascii.hexlify(self.api_key).decode("utf-8")


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
    rows: Sequence[Row]


@dataclass
class Table:
    table_uuid: UUID
    username: str
    table_name: str
    is_public: bool
    caption: str
    data_licence: "DataLicence"
    columns: Sequence["Column"]
    created: datetime

    def has_caption(self) -> bool:
        return len(self.caption.strip()) > 0

    def user_columns(self) -> Sequence["Column"]:
        """Returns 'user_columns' - ie those not owned by csvbase."""
        return [
            column for column in self.columns if not column.name.startswith("csvbase_")
        ]

    def row_id_column(self) -> "Column":
        return self.columns[0]

    def age(self) -> timedelta:
        return self.created - datetime.now(timezone.utc)


@enum.unique
class DataLicence(enum.Enum):
    UNKNOWN = 0
    ALL_RIGHTS_RESERVED = 1
    PDDL = 2
    ODC_BY = 3
    ODBL = 4
    OGL = 5

    def render(self) -> str:
        return _DATA_LICENCE_PP_MAP[self]

    def short_render(self) -> str:
        return _DATA_LICENCE_SHORT_MAP[self]

    def is_free(self) -> bool:
        return self.value > 1


_DATA_LICENCE_PP_MAP = {
    DataLicence.UNKNOWN: "Unknown",
    DataLicence.ALL_RIGHTS_RESERVED: "All rights reserved",
    DataLicence.PDDL: "PDDL (public domain)",
    DataLicence.ODC_BY: "ODB-By (attribution required)",
    DataLicence.ODBL: "ODbl (attribution & sharealike)",
    DataLicence.OGL: "Open Government Licence",
}

_DATA_LICENCE_SHORT_MAP = {
    DataLicence.UNKNOWN: "Unknown",
    DataLicence.ALL_RIGHTS_RESERVED: "All rights reserved",
    DataLicence.PDDL: "Public domain",
    DataLicence.ODC_BY: "ODB-By",
    DataLicence.ODBL: "ODbl",
    DataLicence.OGL: "OGL",
}


@enum.unique
class ColumnType(enum.Enum):
    # These are ints because that int will eventually be used in a table
    # storing columns
    TEXT = 1
    INTEGER = 2
    FLOAT = 3
    BOOLEAN = 4
    DATE = 5

    def example(self) -> "PythonType":
        if self is ColumnType.TEXT:
            return "foo"
        elif self is ColumnType.INTEGER:
            return 1
        elif self is ColumnType.FLOAT:
            return 3.14
        elif self is ColumnType.BOOLEAN:
            return False
        else:
            return date(2018, 1, 3)

    @staticmethod
    def from_sql_type(sqla_type: str) -> "ColumnType":
        return _REVERSE_SQL_TYPE_MAP[sqla_type]

    def sqla_type(self) -> Type["SQLAlchemyType"]:
        """The equivalent SQLAlchemy type"""
        return _SQLA_TYPE_MAP[self]

    def value_to_json(self, value) -> str:
        if self is ColumnType.DATE:
            return value.isoformat()
        else:
            return value

    def from_json_to_python(self, json_value: Any) -> Optional["PythonType"]:
        if self is ColumnType.BOOLEAN:
            return json_value
        elif self is ColumnType.INTEGER:
            return int(json_value)
        elif self is ColumnType.FLOAT:
            return json_value
        elif self is ColumnType.DATE:
            return date.fromisoformat(json_value)
        else:
            return json_value

    def pretty_name(self) -> str:
        return self.name.capitalize()

    def python_type(self) -> Type:
        return _PYTHON_TYPE_MAP[self]

    def pretty_type(self) -> str:
        return _PRETTY_TYPE_MAP[self]


PythonType = Union[int, bool, float, date, str]
SQLAlchemyType = Union[
    satypes.BigInteger,
    satypes.Boolean,
    satypes.Float,
    satypes.Date,
    satypes.Text,
]

_SQLA_TYPE_MAP: Dict["ColumnType", Type[SQLAlchemyType]] = {
    ColumnType.TEXT: satypes.Text,
    ColumnType.INTEGER: satypes.BigInteger,
    ColumnType.FLOAT: satypes.Float,
    ColumnType.BOOLEAN: satypes.Boolean,
    ColumnType.DATE: satypes.Date,
}

_REVERSE_SQL_TYPE_MAP = {
    "boolean": ColumnType.BOOLEAN,
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

_PRETTY_TYPE_MAP = {
    ColumnType.TEXT: "string",
    ColumnType.INTEGER: "integer",
    ColumnType.FLOAT: "float",
    ColumnType.BOOLEAN: "boolean",
    ColumnType.DATE: "date",
}


@dataclass(frozen=True)
class Column:
    name: str
    type_: ColumnType


ROW_ID_COLUMN = Column("csvbase_row_id", type_=ColumnType.INTEGER)


@enum.unique
class ContentType(enum.Enum):
    HTML = "text/html"
    CSV = "text/csv"
    JSON = "application/json"
    PARQUET = "application/parquet"  # this is unofficial, but convenient
    JSON_LINES = ("applicate/x-jsonlines",)  # no consensus
    HTML_FORM = "application/x-www-form-urlencoded"
    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    @classmethod
    def from_file_extension(cls, file_extension: str) -> Optional["ContentType"]:
        return EXTENSION_MAP.get(file_extension)


EXTENSION_MAP: Mapping[str, ContentType] = {
    "html": ContentType.HTML,
    "csv": ContentType.CSV,
    "parquet": ContentType.PARQUET,
    "json": ContentType.JSON,
    "jsonl": ContentType.JSON_LINES,
    "xlsx": ContentType.XLSX,
}
