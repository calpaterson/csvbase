import re
from uuid import UUID
import io
from typing import Optional, Type, List, Iterable, Tuple, Dict
from datetime import datetime, timezone
import csv
from logging import getLogger
from dataclasses import dataclass
from datetime import date
from uuid import uuid4

from pgcopy import CopyManager
from sqlalchemy import table as satable, column as sacolumn, types as satypes
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import TableClause, select
from sqlalchemy.schema import (
    CreateTable,
    Table as SATable,
    Column as SAColumn,
    DropTable,
    MetaData,
)

from .value_objs import KeySet, Page, Column, ColumnType, PythonType
from . import models
from .db import engine
from . import exc

logger = getLogger(__name__)

INT_REGEX = re.compile(r"^\d+$")

FLOAT_REGEX = re.compile(r"^(\d+\.)|(\.\d+)|(\d+\.\d?)$")

BOOL_REGEX = re.compile("^(yes|no|true|false|y|n|t|f)$", re.I)


def types_for_csv(csv_buf, dialect, has_headers=True) -> List[Column]:
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
            rv.append(Column(key, ColumnType.FLOAT))
        elif all(INT_REGEX.match(v) for v in values):
            rv.append(Column(key, ColumnType.INTEGER))
        elif all(BOOL_REGEX.match(v) for v in values):
            rv.append(Column(key, ColumnType.BOOLEAN))
        else:
            rv.append(Column(key, ColumnType.TEXT))
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


def get_columns(sesh, username, table_name, include_row_id=False) -> List["Column"]:
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
        rv.append(Column(name=name, type_=ColumnType.from_sql_type(sql_type)))
    return rv


def get_sqla_table(sesh, username, table_name) -> TableClause:
    columns = get_columns(sesh, username, table_name, include_row_id=True)
    return satable(
        f"{username}__{table_name}",
        *[sacolumn(c.name, type_=c.type_.sqla_type()) for c in columns],
    )


def create_table(
    sesh: Session, username: str, table_name: str, columns: Iterable[Column]
) -> None:
    cols: List[SAColumn] = [
        SAColumn("csvbase_row_id", type_=satypes.BigInteger, primary_key=True)
    ]
    for col in columns:
        cols.append(SAColumn(col.name, type_=col.type_.sqla_type()))
    table = SATable(f"{username}__{table_name}", MetaData(bind=engine), *cols)
    sesh.execute(CreateTable(table))


def upsert_table_metadata(
    sesh: Session, user_uuid: UUID, table_name: str, public: bool
) -> None:
    table_obj = sesh.query(models.Table).get((user_uuid, table_name)) or models.Table(
        user_uuid=user_uuid, table_name=table_name
    )
    table_obj.public = public
    sesh.add(table_obj)


def make_drop_table_ddl(sesh: Session, username: str, table_name: str) -> DropTable:
    sqla_table = get_sqla_table(sesh, username, table_name)
    # sqlalchemy-stubs doesn't match sqla 1.4
    return DropTable(sqla_table)  # type: ignore


def upsert_table(
    sesh: Session,
    user_uuid,
    username,
    table_name: str,
    csv_buf: io.StringIO,
    public=False,
) -> None:
    try:
        dialect = csv.Sniffer().sniff(csv_buf.read(1024))
    except csv.Error:
        logger.warning("unable to sniff dialect, falling back to excel")
        dialect = csv.excel
    logger.info("sniffed dialect: %s", dialect)
    csv_buf.seek(0)
    columns = types_for_csv(csv_buf, dialect)
    csv_buf.seek(0)

    already_exists = table_exists(sesh, user_uuid, table_name)
    if already_exists:
        # FIXME: could truncate or delete all here to save time
        sesh.execute(make_drop_table_ddl(sesh, username, table_name))
        logger.info("dropped %s/%s", username, table_name)
    else:
        upsert_table_metadata(sesh, user_uuid, table_name, public)

    create_table(sesh, username, table_name, columns)
    logger.info(
        "%s %s/%s", "(re)created" if already_exists else "created", username, table_name
    )

    # Copy in with binary copy
    reader = csv.reader(csv_buf, dialect)
    csv_buf.readline()  # pop the header, which is not useful
    row_gen = (
        [col.type_.python_type()(v) for col, v in zip(columns, line)] for line in reader
    )

    raw_conn = sesh.connection().connection
    cols = [c.name for c in get_columns(sesh, username, table_name)]
    copy_manager = CopyManager(raw_conn, f"{username}__{table_name}", cols)
    copy_manager.copy(row_gen)


def table_as_csv(sesh: Session, user_uuid, username, table_name) -> io.StringIO:
    csv_buf = io.StringIO()

    columns = [c.name for c in get_columns(sesh, username, table_name)]

    # this allows for putting the columns in with proper csv escaping
    writer = csv.writer(csv_buf)
    writer.writerow(columns)

    # FIXME: This is probably too slow
    for row in table_as_rows(sesh, user_uuid, username, table_name):
        writer.writerow(row)

    csv_buf.seek(0)
    return csv_buf


def table_as_rows(sesh, user_uuid, username, table_name):
    table = get_sqla_table(sesh, username, table_name)
    q = table.select().order_by(table.c.csvbase_row_id)
    yield from sesh.execute(q)


def table_page(
    sesh: Session, user_uuid: UUID, username: str, table_name: str, keyset: KeySet
) -> Page:
    """Get a page from a table based on the provided KeySet"""
    # FIXME: this doesn't handle empty tables
    table = get_sqla_table(sesh, username, table_name)
    if keyset.op == "greater_than":
        where_cond = table.c.csvbase_row_id > keyset.n
    else:
        where_cond = table.c.csvbase_row_id < keyset.n

    keyset_page = table.select().where(where_cond).limit(keyset.size)

    if keyset.op == "greater_than":
        keyset_page = keyset_page.order_by(table.c.csvbase_row_id)
    else:
        # if we're going backwards we need to reverse the order via a subquery
        keyset_page = keyset_page.order_by(table.c.csvbase_row_id.desc())
        keyset_sub = select(keyset_page.alias())  # type: ignore
        keyset_page = keyset_sub.order_by("csvbase_row_id")

    rows = list(sesh.execute(keyset_page))

    if len(rows) > 1:
        has_more_q = (
            table.select().where(table.c.csvbase_row_id > rows[-1].csvbase_row_id).exists()  # type: ignore
        )
        has_more = sesh.query(has_more_q).scalar()
        has_less_q = (
            table.select().where(table.c.csvbase_row_id < rows[0].csvbase_row_id).exists()  # type: ignore
        )
        has_less = sesh.query(has_less_q).scalar()
    else:
        if keyset.op == "greater_than":
            has_more = False
            has_less = sesh.query(
                table.select().where(table.c.csvbase_row_id < keyset.n).exists()  # type: ignore
            ).scalar()
        else:
            has_more = sesh.query(
                table.select().where(table.c.csvbase_row_id > keyset.n).exists()  # type: ignore
            ).scalar()
            has_less = False
    return Page(
        has_less=has_less,
        has_more=has_more,
        rows=rows[: keyset.size],
    )


def get_row(
    sesh: Session, username: str, table_name: str, row_id: int
) -> Dict[Column, PythonType]:
    columns = get_columns(sesh, username, table_name, include_row_id=False)
    table = get_sqla_table(sesh, username, table_name)
    cursor = sesh.execute(table.select().where(table.c.csvbase_row_id == row_id))
    row = cursor.fetchone()
    if row is None:
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    else:
        return {c: row[c.name] for c in columns}


def update_row(
    sesh: Session,
    username: str,
    table_name: str,
    row_id: int,
    values: Dict[str, PythonType],
) -> bool:
    """Update a given row, returning True if it existed (and was updated) and False otherwise."""
    table = get_sqla_table(sesh, username, table_name)
    result = sesh.execute(
        table.update().where(table.c.csvbase_row_id == row_id).values(values)
    )
    return result.rowcount > 0


def delete_row(sesh: Session, username: str, table_name: str, row_id: int) -> bool:
    """Update a given row, returning True if it existed (and was updated) and False otherwise."""
    table = get_sqla_table(sesh, username, table_name)
    result = sesh.execute(table.delete().where(table.c.csvbase_row_id == row_id))
    return result.rowcount > 0


def insert_row(sesh: Session, username: str, table_name: str, values: Dict) -> int:
    table = get_sqla_table(sesh, username, table_name)
    return sesh.execute(
        table.insert().values(values).returning(table.c.csvbase_row_id)
    ).scalar()


def is_public(sesh, username, table_name) -> bool:
    rv = (
        sesh.query(models.Table.public)
        .join(models.User)
        .filter(models.User.username == username, models.Table.table_name == table_name)
        .scalar()
    )
    if rv is None:
        raise exc.TableDoesNotExistException(username, table_name)
    else:
        return rv


def user_exists(sesh: Session, username: str) -> None:
    exists = sesh.query(
        sesh.query(models.User).filter(models.User.username == username).exists()
    ).scalar()
    if not exists:
        raise exc.UserDoesNotExistException(username)


def create_user(
    sesh, crypt_context, username, password_plain, email: Optional[str] = None
) -> UUID:
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


def is_correct_password(sesh, crypt_context, username, password) -> Optional[bool]:
    user = sesh.query(models.User).filter(models.User.username == username).first()
    if user is None:
        return None
    else:
        return crypt_context.verify(password, user.password_hash)


def tables_for_user(sesh, user_uuid, include_private=False) -> Iterable[str]:
    rs = (
        sesh.query(models.Table.table_name)
        .filter(models.Table.user_uuid == user_uuid)
        .order_by(models.Table.created.desc())
    )
    if not include_private:
        rs = rs.filter(models.Table.public)
    for (table_name,) in rs:
        yield table_name
