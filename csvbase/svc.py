import re
import io
import itertools
from typing import Optional, Dict, Type, List
from datetime import datetime, timezone
import csv
from logging import getLogger
from dataclasses import dataclass
from datetime import date
from uuid import uuid4

from pgcopy import CopyManager
from sqlalchemy import table as satable, column as sacolumn, types as satypes
from sqlalchemy.orm import Session
from sqlalchemy.schema import (
    CreateTable,
    Table as SATable,
    Column as SAColumn,
    DropTable,
    MetaData,
)

from . import models
from .db import engine

logger = getLogger(__name__)

# FIXME: the capitalised ones probably don't work
SQL_TO_PYTHON_TYPEMAP = {
    "bigint": int,
    "text": str,
    "TIMESTAMP WITH TIMEZONE": datetime,
    "double precision": float,
    "BOOLEAN": bool,
    "date": date,
}
# FIXME: shouldn't be needed, no small ints
SQL_TO_PYTHON_TYPEMAP["integer"] = int

INT_REGEX = re.compile("^\d+$")

FLOAT_REGEX = re.compile("^(\d+\.)|(\.\d+)|(\d+\.\d?)$")

BOOL_REGEX = re.compile("^(yes|no|true|false|y|n|t|f)$", re.I)


def types_for_csv(csv_buf, dialect, has_headers=True) -> List["Column"]:
    # FIXME: This should return a list of Column
    # look just at the first 5 lines - that hopefully is easy to explain
    reader = csv.reader(csv_buf, dialect)
    headers = next(reader)
    first_five = zip(*(row for row, _ in zip(reader, range(5))))
    as_dict = dict(zip(headers, first_five))
    rv = []
    # FIXME: add support for dates here... (probably using date-util)
    for key, values in as_dict.items():
        if all(FLOAT_REGEX.match(v) for v in values):
            rv.append(Column(key, float))
        elif all(INT_REGEX.match(v) for v in values):
            rv.append(Column(key, int))
        elif all(BOOL_REGEX.match(v) for v in values):
            rv.append(Column(key, bool))
        else:
            rv.append(Column(key, str))
    logger.info("inferred: %s", rv)
    return rv


def user_uuid_for_name(sesh, username):
    return (
        sesh.query(models.User.user_uuid)
        .filter(models.User.username == username)
        .scalar()
    )


def username_from_user_uuid(sesh, user_uuid):
    return (
        sesh.query(models.User.username)
        .filter(models.User.user_uuid == user_uuid)
        .scalar()
    )


def table_exists(sesh, user_uuid, table_name):
    return sesh.query(
        sesh.query(models.Table)
        .filter(
            models.Table.user_uuid == user_uuid, models.Table.table_name == table_name
        )
        .exists()
    ).scalar()


@dataclass(frozen=True)
class Column:
    name: str
    python_type: type

    def pretty_type(self):
        MAP = {
            int: "integer",
            str: "text",
            float: "float",
            bool: "boolean",
            date: "date",
        }
        return MAP[self.python_type]

    def sqla_type(self):
        MAP = {
            int: satypes.BigInteger,
            str: satypes.Text,
            float: satypes.Float,
            bool: satypes.Boolean,
            date: satypes.Date,
        }
        return MAP[self.python_type]

    def html_type(self):
        MAP = {
            bool: "checkbox",
            date: "date",
        }
        return MAP.get(self.python_type, "text")

    def value_to_json(self, value):
        if self.python_type in [date]:
            return value.isoformat()
        else:
            return value

    def from_string_to_python(self, str):
        if self.python_type is not date:
            return self.python_type(str)
        else:
            return date.fromisoformat(str)


def get_columns(sesh, username, table_name, include_row_id=False):
    # lifted from https://dba.stackexchange.com/a/22420/28877
    stmt = f"""
    SELECT attname AS column_name, atttypid::regtype AS sql_type
    FROM   pg_attribute
    WHERE  attrelid = 'public.{username}__{table_name}'::regclass
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum;
    """
    rs = sesh.execute(stmt)
    rv = []
    for name, sql_type in rs:
        if name == "csvbase_row_id" and not include_row_id:
            continue
        rv.append(Column(name=name, python_type=SQL_TO_PYTHON_TYPEMAP[sql_type]))
    return rv


def get_sqla_table(sesh, username, table_name):
    columns = get_columns(sesh, username, table_name, include_row_id=True)
    return satable(
        f"{username}__{table_name}",
        *[sacolumn(c.name, type_=c.sqla_type()) for c in columns],
    )


def make_create_table_ddl(
    username: str, table_name: str, columns: List[Column]
) -> CreateTable:
    cols: List[SAColumn] = [
        SAColumn("csvbase_row_id", type_=satypes.BigInteger, primary_key=True)
    ]
    for col in columns:
        cols.append(SAColumn(col.name, type_=col.sqla_type()))
    table = SATable(f"{username}__{table_name}", MetaData(bind=engine), *cols)
    return CreateTable(table)


def make_drop_table_ddl(sesh: Session, username: str, table_name: str) -> DropTable:
    sqla_table = get_sqla_table(sesh, username, table_name)
    return DropTable(sqla_table)


def upsert_table(sesh, user_uuid, username, table_name, csv_buf):
    try:
        dialect = csv.Sniffer().sniff(csv_buf.read(1024))
    except csv.Error:
        logger.warning("unable to sniff dialect, falling back to excel")
        dialect = csv.excel
    logger.info("sniffed dialect: %s", dialect)
    csv_buf.seek(0)
    types = types_for_csv(csv_buf, dialect)
    csv_buf.seek(0)

    already_exists = table_exists(sesh, user_uuid, table_name)
    if already_exists:
        # FIXME: could truncate or delete all here to save time
        sesh.execute(make_drop_table_ddl(sesh, username, table_name))
        logger.info("dropped %s/%s", username, table_name)
    else:
        sesh.add(models.Table(user_uuid=user_uuid, table_name=table_name, public=True))

    sesh.execute(make_create_table_ddl(username, table_name, types))
    logger.info(
        "%s %s/%s", "(re)created" if already_exists else "created", username, table_name
    )

    # Copy in with binary copy
    reader = csv.reader(csv_buf, dialect)
    csv_buf.readline()  # pop the header, which is not useful
    row_gen = (line for line in reader)

    raw_conn = sesh.connection().connection
    cols = [c.name for c in get_columns(sesh, username, table_name)]
    copy_manager = CopyManager(raw_conn, f"{username}__{table_name}", cols)
    copy_manager.copy(row_gen)


def table_as_csv(sesh, user_uuid, username, table_name):
    csv_buf = io.StringIO()

    columns = [c.name for c in get_columns(sesh, username, table_name)]

    # this allows for putting the columns in with proper csv escaping
    header_writer = csv.writer(csv_buf)
    header_writer.writerow(columns)

    cursor = sesh.connection().connection.cursor()
    cursor.copy_to(csv_buf, f"{username}__{table_name}", sep=",", columns=columns)
    csv_buf.seek(0)
    return csv_buf


def table_as_rows(sesh, user_uuid, username, table_name):
    table = get_sqla_table(sesh, username, table_name)
    yield from sesh.execute(table.select().order_by(table.c.csvbase_row_id))


def get_row(sesh, username, table_name, row_id):
    columns = get_columns(sesh, username, table_name, include_row_id=False)
    table = get_sqla_table(sesh, username, table_name)
    cursor = sesh.execute(table.select().where(table.c.csvbase_row_id == row_id))
    row = cursor.fetchone()

    return {c: row[c.name] for c in columns}


def update_row(sesh, username, table_name, row_id, values):
    table = get_sqla_table(sesh, username, table_name)
    sesh.execute(table.update().where(table.c.csvbase_row_id == row_id).values(values))


def is_public(sesh, username, table_name):
    return (
        sesh.query(models.Table.public)
        .join(models.User)
        .filter(models.User.username == username, models.Table.table_name == table_name)
        .scalar()
    )


def create_user(sesh, crypt_context, username, password_plain, email: Optional[str]):
    user_uuid = uuid4()
    password_hashed = crypt_context.hash(password_plain)
    registered = datetime.now(timezone.utc)
    user = models.User(
        user_uuid=user_uuid,
        username=username,
        password_hash=password_hashed,
        # FIXME: hardcoded default
        timezone="Europe/London",
        registered=registered,
    )

    if email is not None:
        user.email_obj = models.UserEmail(email_address=email)

    sesh.add(user)

    return user_uuid


def is_correct_password(sesh, crypt_context, username, password):
    user = sesh.query(models.User).filter(models.User.username == username).first()
    if user is None:
        return None
    else:
        return crypt_context.verify(password, user.password_hash)


def tables_for_user(sesh, user_uuid, include_private=False):
    rs = sesh.query(models.Table.table_name).filter(models.Table.user_uuid == user_uuid)
    if not include_private:
        rs = rs.filter(models.Table.public)
    for (table_name,) in rs:
        yield table_name
