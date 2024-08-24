from collections import defaultdict
from logging import getLogger
from typing import (
    Optional,
    Sequence,
    Type,
    Dict,
    Union,
    Mapping,
    List,
    Tuple,
    Set,
    cast,
    Any,
    IO,
    Iterable,
)
from typing_extensions import Literal
from uuid import UUID
from datetime import datetime, date, timedelta, timezone
from dataclasses import (
    dataclass,
    asdict as dataclass_as_dict,
)
import enum
import binascii
import encodings.aliases
import importlib_resources
import csv

import giturlparse
from dateutil.tz import gettz
from sqlalchemy import types as satypes

logger = getLogger(__name__)

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
    timezone: str
    mailing_list: bool

    def hex_api_key(self) -> str:
        return binascii.hexlify(self.api_key).decode("utf-8")

    def tzfile(self) -> Any:
        """Returns the timezone "object" which you can pass as an argument into
        datetime.replace or datetime.now."""
        try:
            return gettz(self.timezone)
        except Exception:
            logger.exception("unable to load timezone for user, using UTC")
            return timezone.utc

    def email_for_web_templates(self) -> str:
        return self.email or ""


@dataclass
class KeySet:
    """Used as a selector for keyset pagination

    https://use-the-index-luke.com/no-offset

    """

    columns: List["Column"]
    values: Tuple
    op: Literal["greater_than", "less_than"]
    size: int = 10


# sketch for filters
# @dataclass
# class KeySetNG:
#     filters: Sequence["BinaryFilter"]
#     size: Optional[int] = 10


# @dataclass
# class BinaryFilter:
#     lhs: Union["Column"]
#     rhs: Union["Column", "PythonType"]
#     op: "BinaryOp"


@enum.unique
class BinaryOp(enum.Enum):
    EQ = "eq"
    NQE = "nqe"
    GT = "gt"
    GTE = "gte"
    LT = "lt"
    LTE = "lte"


@dataclass
class Page:
    """A page from a table"""

    # FIXME: This is pretty awful as an API.  eg it would be great to know if
    # row id X was in the page

    has_less: bool
    has_more: bool
    rows: Sequence[Row]

    def row_ids(self) -> Set[int]:
        return cast(Set[int], {row[ROW_ID_COLUMN] for row in self.rows})

    def row_count(self) -> int:
        return len(self.rows)


@dataclass
class RowCount:
    exact: Optional[int]
    approx: int

    def best(self) -> int:
        return self.exact or self.approx

    def is_big(self) -> bool:
        # Big Data == "too big for excel"
        return self.best() > 1_048_576


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
    row_count: RowCount
    last_changed: datetime
    key: Optional[Sequence["Column"]]
    upstream: Optional["GitUpstream"] = None

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

    def ref(self) -> str:
        return f"{self.username}/{self.table_name}"

    @property
    def licence(self) -> Optional["Licence"]:
        return Licence.from_data_licence(self.data_licence)


@dataclass
class GitUpstream:
    last_modified: datetime
    last_sha: bytes
    repo_url: str
    branch: str
    path: str

    def _parsed_url(self):
        return giturlparse.parse(self.repo_url)

    def version(self) -> "UpstreamVersion":
        return UpstreamVersion(self.last_modified, self.last_sha.hex())

    def pretty_ref(self) -> str:
        as_https_url = self._parsed_url().url2https
        return f"git+{as_https_url}@{self.branch}"

    def github_file_link(self) -> str:
        """Return a url to the upstream file on github.com."""
        url = self._parsed_url().url2https[:-4]
        url += f"/blob/{self.branch}/{self.path}"
        return url

    def github_commit_link(self) -> str:
        """Return a url to the last commit we know of on github.com."""
        base_url = self._parsed_url().url2https[:-4]
        return f"{base_url}/commit/{self.last_sha.hex()}"

    def to_json_dict(self) -> Dict[str, Any]:
        json_dict = dataclass_as_dict(self)
        json_dict["last_sha"] = json_dict["last_sha"].hex()
        json_dict["last_modified"] = json_dict["last_modified"].isoformat()
        return json_dict

    @staticmethod
    def from_json_dict(json_dict: Dict[str, Any]) -> "GitUpstream":
        # no mutations
        parsed: Dict[str, Any] = {}
        parsed["last_sha"] = bytes.fromhex(json_dict["last_sha"])
        parsed["last_modified"] = datetime.fromisoformat(json_dict["last_modified"])
        return GitUpstream(**{**json_dict, **parsed})

    def is_read_only(self) -> bool:
        return self._parsed_url().username == ""


@dataclass
class UpstreamVersion:
    """Represents a version of a file in an external system.

    For git it is commit_date and commit sha.
    """

    last_changed: datetime
    version_id: str


@dataclass
class UpstreamFile:
    version: UpstreamVersion
    filelike: IO[bytes]


@enum.unique
class DataLicence(enum.Enum):
    # Deprecated enum - only able to represent a limited number of licences
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


@dataclass(frozen=True, eq=True)  # FIXME: set slots=True when 3.10+
class Licence:
    """Represents a licence."""

    spdx_id: str
    name: str
    osi_approved: bool

    @staticmethod
    def from_data_licence(data_licence: DataLicence) -> Optional["Licence"]:
        return _DATA_LICENCE_TO_LICENCE_MAP.get(data_licence, None)


def build_licence_map() -> Iterable[Tuple[str, Licence]]:
    with importlib_resources.files("csvbase").joinpath("data/spdx-licences.csv").open(
        "r"
    ) as spdx_licences_f:
        reader = csv.DictReader(spdx_licences_f)
        for row in reader:
            licence = Licence(
                spdx_id=row["licenseId"],
                name=row["name"],
                osi_approved=True if row["isOsiApproved"] == "True" else False,
            )
            yield licence.spdx_id, licence


LICENCE_MAP: Mapping[str, Licence] = dict(build_licence_map())


_DATA_LICENCE_TO_LICENCE_MAP = {
    DataLicence.PDDL: LICENCE_MAP["PDDL-1.0"],
    DataLicence.ODC_BY: LICENCE_MAP["ODC-By-1.0"],
    DataLicence.ODBL: LICENCE_MAP["ODbL-1.0"],
    DataLicence.OGL: LICENCE_MAP["OGL-UK-3.0"],
}

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

    def pretty_name(self) -> str:
        """The presentation name of the type.  Intended for UIs."""
        return self.name.capitalize()

    def python_type(self) -> Type:
        return _PYTHON_TYPE_MAP[self]

    def pretty_type(self) -> str:
        """The "pretty" name of the type.  Intended for APIs."""
        return _PRETTY_TYPE_MAP[self]


PythonType = Union[int, bool, float, date, str, None]
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
    CSV = "text/csv"
    HTML = "text/html"
    HTML_FORM = "application/x-www-form-urlencoded"
    JSON = "application/json"
    JSON_LINES = "application/x-jsonlines"  # no consensus
    PARQUET = "application/parquet"  # this is unofficial, but convenient
    XLSX = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    MARKDOWN = "text/markdown"

    @classmethod
    def from_file_extension(cls, file_extension: str) -> Optional["ContentType"]:
        return EXTENSION_MAP.get(file_extension)

    def pretty_name(self) -> str:
        return PRETTY_NAME_MAP[self]

    def file_extension(self) -> str:
        return EXTENSION_MAP_REVERSE[self]


EXTENSION_MAP: Mapping[str, ContentType] = {
    "html": ContentType.HTML,
    "csv": ContentType.CSV,
    "parquet": ContentType.PARQUET,
    "json": ContentType.JSON,
    "jsonl": ContentType.JSON_LINES,
    "xlsx": ContentType.XLSX,
    "md": ContentType.MARKDOWN,
}

EXTENSION_MAP_REVERSE = {v: k for k, v in EXTENSION_MAP.items()}


PRETTY_NAME_MAP: Mapping[ContentType, str] = {
    ContentType.HTML: "HTML",
    ContentType.CSV: "CSV",
    ContentType.PARQUET: "Parquet",
    ContentType.JSON: "JSON",
    ContentType.JSON_LINES: "JSON lines",
    ContentType.XLSX: "MS Excel",
    ContentType.MARKDOWN: "Markdown",
}


@dataclass
class Quota:
    private_tables: int
    private_bytes: int


@dataclass
class Usage:
    """Represents the actual usage of a user - to be compared against their
    Quota.

    """

    public_tables: int
    public_bytes: int
    private_tables: int
    private_bytes: int

    def exceeds_quota(self, quota: Quota) -> bool:
        return (self.private_tables > quota.private_tables) or (
            self.private_bytes > quota.private_bytes
        )


@enum.unique
class Encoding(enum.Enum):
    ASCII = "ascii"
    BIG5 = "big5"
    BIG5HKSCS = "big5hkscs"
    CP037 = "cp037"
    CP273 = "cp273"
    CP424 = "cp424"
    CP437 = "cp437"
    CP500 = "cp500"
    CP720 = "cp720"
    CP737 = "cp737"
    CP775 = "cp775"
    CP850 = "cp850"
    CP852 = "cp852"
    CP855 = "cp855"
    CP856 = "cp856"
    CP857 = "cp857"
    CP858 = "cp858"
    CP860 = "cp860"
    CP861 = "cp861"
    CP862 = "cp862"
    CP863 = "cp863"
    CP864 = "cp864"
    CP865 = "cp865"
    CP866 = "cp866"
    CP869 = "cp869"
    CP874 = "cp874"
    CP875 = "cp875"
    CP932 = "cp932"
    CP949 = "cp949"
    CP950 = "cp950"
    CP1006 = "cp1006"
    CP1026 = "cp1026"
    CP1125 = "cp1125"
    CP1140 = "cp1140"
    CP1250 = "cp1250"
    CP1251 = "cp1251"
    CP1252 = "cp1252"
    CP1253 = "cp1253"
    CP1254 = "cp1254"
    CP1255 = "cp1255"
    CP1256 = "cp1256"
    CP1257 = "cp1257"
    CP1258 = "cp1258"
    EUC_JP = "euc_jp"
    EUC_JIS_2004 = "euc_jis_2004"
    EUC_JISX0213 = "euc_jisx0213"
    EUC_KR = "euc_kr"
    GB2312 = "gb2312"
    GBK = "gbk"
    GB18030 = "gb18030"
    HZ = "hz"
    ISO2022_JP = "iso2022_jp"
    ISO2022_JP_1 = "iso2022_jp_1"
    ISO2022_JP_2 = "iso2022_jp_2"
    ISO2022_JP_2004 = "iso2022_jp_2004"
    ISO2022_JP_3 = "iso2022_jp_3"
    ISO2022_JP_EXT = "iso2022_jp_ext"
    ISO2022_KR = "iso2022_kr"
    LATIN_1 = "latin_1"
    ISO8859_2 = "iso8859_2"
    ISO8859_3 = "iso8859_3"
    ISO8859_4 = "iso8859_4"
    ISO8859_5 = "iso8859_5"
    ISO8859_6 = "iso8859_6"
    ISO8859_7 = "iso8859_7"
    ISO8859_8 = "iso8859_8"
    ISO8859_9 = "iso8859_9"
    ISO8859_10 = "iso8859_10"
    ISO8859_11 = "iso8859_11"
    ISO8859_13 = "iso8859_13"
    ISO8859_14 = "iso8859_14"
    ISO8859_15 = "iso8859_15"
    ISO8859_16 = "iso8859_16"
    JOHAB = "johab"
    KOI8_R = "koi8_r"
    KOI8_T = "koi8_t"
    KOI8_U = "koi8_u"
    KZ1048 = "kz1048"
    MAC_CYRILLIC = "mac_cyrillic"
    MAC_GREEK = "mac_greek"
    MAC_ICELAND = "mac_iceland"
    MAC_LATIN2 = "mac_latin2"
    MAC_ROMAN = "mac_roman"
    MAC_TURKISH = "mac_turkish"
    PTCP154 = "ptcp154"
    SHIFT_JIS = "shift_jis"
    SHIFT_JIS_2004 = "shift_jis_2004"
    SHIFT_JISX0213 = "shift_jisx0213"
    UTF_32 = "utf_32"
    UTF_32_BE = "utf_32_be"
    UTF_32_LE = "utf_32_le"
    UTF_16 = "utf_16"
    UTF_16_BE = "utf_16_be"
    UTF_16_LE = "utf_16_le"
    UTF_7 = "utf_7"
    UTF_8 = "utf_8"
    UTF_8_SIG = "utf_8_sig"

    @property
    def aliases(self) -> Sequence[str]:
        return ENCODING_ALIASES_MAP[self.value]


_encoding_aliases = defaultdict(list)
for alias, encoding_value in encodings.aliases.aliases.items():
    _encoding_aliases[encoding_value].append(alias)

ENCODING_ALIASES_MAP = {
    encoding.value: sorted(_encoding_aliases[encoding.value]) for encoding in Encoding
}


@enum.unique
class Backend(enum.Enum):
    POSTGRES = 1
    HTTP = 2
    GIT = 3


@dataclass
class TableRepresentation:
    """Convenience object holding metadata on a specific representation (eg:
    csv) of a table."""

    content_type: ContentType
    offered: bool
    size: int
    size_is_estimate: bool


@dataclass
class Comment:
    comment_id: int
    user: User
    created: datetime
    updated: datetime
    markdown: str


@dataclass
class Thread:
    slug: str
    title: str
    created: datetime
    updated: datetime
