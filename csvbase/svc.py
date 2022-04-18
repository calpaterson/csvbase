import re
from uuid import UUID
import io
from typing import (
    Optional,
    List,
    Iterable,
    Tuple,
    Dict,
    Any,
    Union,
    Sequence,
    Type,
    TYPE_CHECKING,
)
from datetime import datetime, timezone
import csv
from logging import getLogger
from uuid import uuid4
import secrets
import binascii

import xlsxwriter
from pgcopy import CopyManager
from sqlalchemy import column as sacolumn, types as satypes
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import (
    TableClause,
    select,
    TextClause,
    text,
    table as satable,
)
from sqlalchemy.schema import (
    CreateTable,
    Table as SATable,
    Column as SAColumn,
    DropTable,
    MetaData,
)

from .value_objs import (
    KeySet,
    Page,
    Column,
    ColumnType,
    PythonType,
    User,
    Table,
    DataLicence,
    UserSubmittedCSVData,
    Row,
)
from . import models
from .db import engine
from . import exc

if TYPE_CHECKING:
    from sqlalchemy.engine import RowProxy

logger = getLogger(__name__)

INT_REGEX = re.compile(r"^\d+$")

FLOAT_REGEX = re.compile(r"^(\d+\.)|(\.\d+)|(\d+\.\d?)$")

BOOL_REGEX = re.compile("^(yes|no|true|false|y|n|t|f)$", re.I)


def types_for_csv(
    csv_buf: UserSubmittedCSVData,
) -> Tuple[Type[csv.Dialect], List[Column]]:
    # FIXME: should be called "peek csv" or similar
    try:
        dialect = csv.Sniffer().sniff(csv_buf.read(1024))
    except csv.Error:
        logger.warning("unable to sniff dialect, falling back to excel")
        dialect = csv.excel
    logger.info("sniffed dialect: %s", dialect)
    csv_buf.seek(0)

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
    return dialect, rv


def user_by_name(sesh, username: str) -> User:
    sqla_user = sesh.query(models.User).filter(models.User.username == username).first()
    if sqla_user is None:
        raise exc.UserDoesNotExistException(username)
    else:
        if sqla_user.email_obj is not None:
            email = sqla_user.email_obj.email_address
        else:
            email = None
        return User(
            user_uuid=sqla_user.user_uuid,
            username=username,
            registered=sqla_user.registered,
            api_key=sqla_user.api_key.api_key,
            email=email,
        )


def user_by_user_uuid(sesh, user_uuid: UUID) -> User:
    sqla_user = (
        sesh.query(models.User).filter(models.User.user_uuid == user_uuid).first()
    )
    if sqla_user is None:
        raise exc.UserDoesNotExistException(str(user_uuid))
    else:
        if sqla_user.email_obj is not None:
            email = sqla_user.email_obj.email_address
        else:
            email = None
        return User(
            user_uuid=user_uuid,
            username=sqla_user.username,
            registered=sqla_user.registered,
            api_key=sqla_user.api_key.api_key,
            email=email,
        )


def table_exists(sesh: Session, user_uuid: UUID, table_name: str) -> bool:
    return sesh.query(
        sesh.query(models.Table)
        .filter(
            models.Table.user_uuid == user_uuid, models.Table.table_name == table_name
        )
        .exists()
    ).scalar()


def get_table(sesh, username_or_uuid: Union[UUID, str], table_name) -> Table:
    if isinstance(username_or_uuid, str):
        user = user_by_name(sesh, username_or_uuid)
    else:
        user = user_by_user_uuid(sesh, username_or_uuid)

    table_model: Optional[models.Table] = (
        sesh.query(models.Table)
        .filter(
            models.Table.user_uuid == user.user_uuid,
            models.Table.table_name == table_name,
        )
        .first()
    )
    if table_model is None:
        raise exc.TableDoesNotExistException(user.username, table_name)
    columns = get_columns(sesh, user.username, table_name)
    table = Table(
        table_name=table_name,
        is_public=table_model.public,
        caption=table_model.caption,
        data_licence=DataLicence(table_model.licence_id),
        columns=columns,
    )
    return table


def get_columns(sesh, username, table_name) -> List["Column"]:
    # lifted from https://dba.stackexchange.com/a/22420/28877
    attrelid = f"userdata.{username}__{table_name}"
    stmt = text(
        """
    SELECT attname AS column_name, atttypid::regtype AS sql_type
    FROM   pg_attribute
    WHERE  attrelid = :attrelid ::regclass
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum
    """
    )
    rs = sesh.execute(stmt, {"attrelid": attrelid})
    rv = []
    for name, sql_type in rs:
        rv.append(Column(name=name, type_=ColumnType.from_sql_type(sql_type)))
    return rv


def get_userdata_tableclause(sesh, username, table_name) -> TableClause:
    columns = get_columns(sesh, username, table_name)
    return satable(  # type: ignore
        f"{username}__{table_name}",
        *[sacolumn(c.name, type_=c.type_.sqla_type()) for c in columns],
        schema="userdata",
    )


def create_table(
    sesh: Session, username: str, table_name: str, columns: Iterable[Column]
) -> None:
    cols: List[SAColumn] = [
        SAColumn("csvbase_row_id", type_=satypes.BigInteger, primary_key=True),
        # FIXME: would be good to have these two columns plus my
        # "csvbase_created_by" and csvbase_updated_by, but needs support for
        # datetimes as a type
        # SAColumn(
        #     "csvbase_created",
        #     type_=satypes.TIMESTAMP(timezone=True),
        #     nullable=False,
        #     default="now()",
        # ),
        # SAColumn(
        #     "csvbase_update",
        #     type_=satypes.TIMESTAMP(timezone=True),
        #     nullable=False,
        #     default="now()",
        # ),
    ]
    for col in columns:
        cols.append(SAColumn(col.name, type_=col.type_.sqla_type()))
    table = SATable(
        f"{username}__{table_name}", MetaData(bind=engine), *cols, schema="userdata"
    )
    sesh.execute(CreateTable(table))


def upsert_table_metadata(
    sesh: Session,
    user_uuid: UUID,
    table_name: str,
    is_public: bool,
    caption: str,
    licence: DataLicence,
) -> None:
    table_obj: Optional[models.Table] = (
        sesh.query(models.Table)
        .filter(
            models.Table.user_uuid == user_uuid, models.Table.table_name == table_name
        )
        .first()
    )
    if table_obj is None:
        table_obj = models.Table(user_uuid=user_uuid, table_name=table_name)
        sesh.add(table_obj)
    table_obj.public = is_public
    table_obj.caption = caption
    table_obj.licence_id = licence.value


def make_drop_table_ddl(sesh: Session, username: str, table_name: str) -> DropTable:
    sqla_table = get_userdata_tableclause(sesh, username, table_name)
    # sqlalchemy-stubs doesn't match sqla 1.4
    return DropTable(sqla_table)  # type: ignore


def make_truncate_table_ddl(
    sesh: Session, username: str, table_name: str
) -> TextClause:
    return text(f'TRUNCATE "{username}__{table_name}"')


def upsert_table_data(
    sesh: Session,
    user_uuid: UUID,
    username: str,
    table_name: str,
    csv_buf: UserSubmittedCSVData,
    dialect: Type[csv.Dialect],
    columns: Sequence[Column],
    truncate_first=True,
) -> None:
    if truncate_first:
        sesh.execute(make_truncate_table_ddl(sesh, username, table_name))
        logger.info("truncated %s/%s", username, table_name)

    # Copy in with binary copy
    reader = csv.reader(csv_buf, dialect)
    csv_buf.readline()  # pop the header, which is not useful
    row_gen = (
        [col.type_.from_string_to_python(v) for col, v in zip(columns, line)]
        for line in reader
    )

    table = get_table(sesh, username, table_name)
    raw_conn = sesh.connection().connection
    cols = [c.name for c in table.user_columns()]
    copy_manager = CopyManager(raw_conn, f"userdata.{username}__{table_name}", cols)
    copy_manager.copy(row_gen)


def table_as_csv(
    sesh: Session,
    user_uuid: UUID,
    username: str,
    table_name: str,
    delimiter: str = ",",
) -> io.StringIO:
    csv_buf = io.StringIO()

    columns = [c.name for c in get_columns(sesh, username, table_name)]

    # this allows for putting the columns in with proper csv escaping
    writer = csv.writer(csv_buf, delimiter=delimiter)
    writer.writerow(columns)

    # FIXME: This is probably too slow
    for row in table_as_rows(sesh, user_uuid, username, table_name):
        writer.writerow(row)

    csv_buf.seek(0)
    return csv_buf


def table_as_xlsx(
    sesh: Session,
    user_uuid: UUID,
    username: str,
    table_name: str,
    excel_table: bool = False,
) -> io.BytesIO:
    xlsx_buf = io.BytesIO()

    column_names = [c.name for c in get_columns(sesh, username, table_name)]

    rows = table_as_rows(sesh, user_uuid, username, table_name)

    workbook_args = {}
    if not excel_table:
        workbook_args["constant_memory"] = True

    with xlsxwriter.Workbook(xlsx_buf, workbook_args) as workbook:
        worksheet = workbook.add_worksheet()

        if excel_table:
            rows = list(rows)
            table_args: Dict[str, Any] = {}
            table_args["data"] = rows
            table_args["columns"] = [
                {"header": column_name} for column_name in column_names
            ]

            worksheet.add_table(
                first_row=0,
                first_col=0,
                last_row=len(rows),
                # FIXME: last_col should be zero indexed, isn't - bug?
                last_col=len(column_names) - 1,
                options=table_args,
            )
        else:
            worksheet.write_row(0, 0, column_names)

            for index, row in enumerate(rows, start=1):
                worksheet.write_row(index, 0, row)

    xlsx_buf.seek(0)
    return xlsx_buf


def table_as_rows(
    sesh: Session,
    user_uuid: UUID,
    username: str,
    table_name: str,
) -> Iterable[Tuple[PythonType]]:
    table_clause = get_userdata_tableclause(sesh, username, table_name)
    columns = get_columns(sesh, username, table_name)
    q = select([getattr(table_clause.c, c.name) for c in columns]).order_by(
        table_clause.c.csvbase_row_id
    )
    yield from sesh.execute(q)


def table_page(sesh: Session, username: str, table: Table, keyset: KeySet) -> Page:
    """Get a page from a table based on the provided KeySet"""
    # FIXME: this doesn't handle empty tables
    table_clause = get_userdata_tableclause(sesh, username, table.table_name)
    if keyset.op == "greater_than":
        where_cond = table_clause.c.csvbase_row_id > keyset.n
    else:
        where_cond = table_clause.c.csvbase_row_id < keyset.n

    keyset_page = table_clause.select().where(where_cond).limit(keyset.size)

    if keyset.op == "greater_than":
        keyset_page = keyset_page.order_by(table_clause.c.csvbase_row_id)
    else:
        # if we're going backwards we need to reverse the order via a subquery
        keyset_page = keyset_page.order_by(table_clause.c.csvbase_row_id.desc())
        keyset_sub = select(keyset_page.alias())  # type: ignore
        keyset_page = keyset_sub.order_by("csvbase_row_id")

    row_tuples: List[RowProxy] = list(sesh.execute(keyset_page))

    if len(row_tuples) > 1:
        has_more_q = (
            table_clause.select().where(table_clause.c.csvbase_row_id > row_tuples[-1].csvbase_row_id).exists()  # type: ignore
        )
        has_more = sesh.query(has_more_q).scalar()
        has_less_q = (
            table_clause.select().where(table_clause.c.csvbase_row_id < row_tuples[0].csvbase_row_id).exists()  # type: ignore
        )
        has_less = sesh.query(has_less_q).scalar()
    else:
        if keyset.op == "greater_than":
            has_more = False
            has_less = sesh.query(
                table_clause.select().where(table_clause.c.csvbase_row_id < keyset.n).exists()  # type: ignore
            ).scalar()
        else:
            has_more = sesh.query(
                table_clause.select().where(table_clause.c.csvbase_row_id > keyset.n).exists()  # type: ignore
            ).scalar()
            has_less = False

    rows = [{c: row_tup[c.name] for c in table.columns} for row_tup in row_tuples]

    return Page(
        has_less=has_less,
        has_more=has_more,
        rows=rows[: keyset.size],
    )


def get_row(sesh: Session, username: str, table_name: str, row_id: int) -> Row:
    columns = get_columns(sesh, username, table_name)
    table_clause = get_userdata_tableclause(sesh, username, table_name)
    cursor = sesh.execute(
        table_clause.select().where(table_clause.c.csvbase_row_id == row_id)
    )
    row = cursor.fetchone()
    if row is None:
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    else:
        return {c: row[c.name] for c in columns}


def get_a_sample_row(sesh: Session, username: str, table_name: str) -> Row:
    """Returns a sample row from the table (the lowest row id).

    If none exist, a made-up row is returned.  This function is for
    example/documentation purposes only."""
    columns = get_columns(sesh, username, table_name)
    table_clause = get_userdata_tableclause(sesh, username, table_name)
    cursor = sesh.execute(table_clause.select().order_by("csvbase_row_id").limit(1))
    row = cursor.fetchone()
    if row is None:
        # return something made-up
        return {c: c.type_.example() for c in columns}
    else:
        return {c: row[c.name] for c in columns}


def get_a_made_up_row(sesh: Session, username, table_name: str) -> Row:
    columns = get_columns(sesh, username, table_name)
    return {c: c.type_.example() for c in columns}


def update_row(
    sesh: Session,
    username: str,
    table_name: str,
    row_id: int,
    row: Row,
) -> bool:
    """Update a given row, returning True if it existed (and was updated) and False otherwise."""
    table = get_userdata_tableclause(sesh, username, table_name)
    values = {c.name: v for c, v in row.items()}
    result = sesh.execute(
        table.update().where(table.c.csvbase_row_id == row_id).values(values)
    )
    return result.rowcount > 0


def delete_row(sesh: Session, username: str, table_name: str, row_id: int) -> bool:
    """Update a given row, returning True if it existed (and was updated) and False otherwise."""
    table = get_userdata_tableclause(sesh, username, table_name)
    result = sesh.execute(table.delete().where(table.c.csvbase_row_id == row_id))
    return result.rowcount > 0


def insert_row(sesh: Session, username: str, table_name: str, row: Row) -> int:
    table = get_userdata_tableclause(sesh, username, table_name)
    values = {c.name: v for c, v in row.items()}
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
    sesh, crypt_context, username: str, password_plain: str, email: Optional[str] = None
) -> User:
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
    user.api_key = models.APIKey(api_key=secrets.token_bytes(16))

    sesh.add(user)

    return User(
        user_uuid=user.user_uuid,
        username=username,
        registered=user.registered,
        api_key=user.api_key.api_key,
        email=email,
    )


def is_correct_password(
    sesh: Session, crypt_context, username: str, password: str
) -> Optional[bool]:
    user = sesh.query(models.User).filter(models.User.username == username).first()
    if user is None:
        return None
    else:
        return crypt_context.verify(password, user.password_hash)


def is_valid_api_key(sesh: Session, username: str, hex_api_key: str) -> bool:
    try:
        api_key = binascii.unhexlify(hex_api_key)
    except binascii.Error:
        raise exc.InvalidAPIKeyException()
    exists = sesh.query(
        sesh.query(models.APIKey)
        .join(models.User)
        .filter(models.User.username == username, models.APIKey.api_key == api_key)
        .exists()
    ).scalar()
    return exists


def tables_for_user(
    sesh: Session, user_uuid: UUID, include_private: bool = False
) -> Iterable[str]:
    rs = (
        sesh.query(models.Table.table_name)
        .filter(models.Table.user_uuid == user_uuid)
        .order_by(models.Table.created.desc())
    )
    if not include_private:
        rs = rs.filter(models.Table.public)
    for (table_name,) in rs:
        yield table_name
