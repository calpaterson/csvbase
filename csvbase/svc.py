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
from contextlib import closing
import importlib.resources

import bleach
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
from sqlalchemy.sql import exists
from sqlalchemy.schema import (  # type: ignore
    CreateTable,
    Table as SATable,
    Column as SAColumn,
    DropTable,
    MetaData,
    Identity,
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
from . import data
from . import conv

if TYPE_CHECKING:
    from sqlalchemy.engine import RowProxy

logger = getLogger(__name__)

FLOAT_REGEX = re.compile(r"^(\d+\.)|(\.\d+)|(\d+\.\d?)$")

BOOL_REGEX = re.compile("^(yes|no|true|false|y|n|t|f)$", re.I)


def peek_csv(
    csv_buf: UserSubmittedCSVData,
) -> Tuple[Type[csv.Dialect], List[Column]]:
    """Infer the csv dialect (usually: excel) and the column names/types) by
    looking at the top of it.

    """
    # FIXME: should be called "peek csv" or similar
    try:
        dialect = csv.Sniffer().sniff(csv_buf.read(1024))
    except csv.Error:
        logger.warning("unable to sniff dialect, falling back to excel")
        dialect = csv.excel
    logger.info("sniffed dialect: %s", dialect)
    csv_buf.seek(0)

    # FIXME: probably should be configurable but larger default - maybe 25?
    # look just at the first 5 lines - that hopefully is easy to explain
    reader = csv.reader(csv_buf, dialect)
    headers = next(reader)
    first_five = zip(*(row for row, _ in zip(reader, range(5))))
    as_dict = dict(zip(headers, first_five))
    rv = []
    dc = conv.DateConverter()
    ic = conv.IntegerConverter()
    fc = conv.FloatConverter()
    bc = conv.BooleanConverter()
    for key, values in as_dict.items():
        if ic.sniff(values):
            rv.append(Column(key, ColumnType.INTEGER))
        elif fc.sniff(values):
            rv.append(Column(key, ColumnType.FLOAT))
        elif bc.sniff(values):
            rv.append(Column(key, ColumnType.BOOLEAN))
        elif dc.sniff(values):
            rv.append(Column(key, ColumnType.DATE))
        else:
            rv.append(Column(key, ColumnType.TEXT))
    logger.info("inferred: %s", rv)
    return dialect, rv


def user_by_name(sesh: Session, username: str) -> User:
    # FIXME: This is quite a hot function, needs some caching
    rp = (
        sesh.query(
            models.User.user_uuid,
            models.User.registered,
            models.APIKey.api_key,
            models.UserEmail.email_address,
        )
        .join(models.APIKey)
        .outerjoin(models.UserEmail)
        .filter(models.User.username == username)
        .first()
    )
    if rp is None:
        raise exc.UserDoesNotExistException(username)
    else:
        user_uuid, registered, api_key, email = rp
        return User(
            user_uuid=user_uuid,
            username=username,
            registered=registered,
            api_key=api_key,
            email=email,
        )


def user_by_user_uuid(sesh, user_uuid: UUID) -> User:
    # FIXME: Again, quite a hot function, needs some caching
    rp = (
        sesh.query(
            models.User.username,
            models.User.registered,
            models.APIKey.api_key,
            models.UserEmail.email_address,
        )
        .join(models.APIKey)
        .outerjoin(models.UserEmail)
        .filter(models.User.user_uuid == user_uuid)
        .first()
    )
    if rp is None:
        raise exc.UserDoesNotExistException(str(user_uuid))
    else:
        username, registered, api_key, email = rp
        return User(
            user_uuid=user_uuid,
            username=username,
            registered=registered,
            api_key=api_key,
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


def get_table(sesh: Session, username: str, table_name: str) -> Table:
    user = user_by_name(sesh, username)
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
    columns = get_columns(sesh, table_model.table_uuid)
    return _table_model_and_columns_to_table(user.username, table_model, columns)


def get_columns(sesh, table_uuid) -> List["Column"]:
    # lifted from https://dba.stackexchange.com/a/22420/28877
    attrelid = make_userdata_table_name(table_uuid, with_schema=True)
    stmt = text(
        """
    SELECT attname AS column_name, atttypid::regtype AS sql_type
    FROM   pg_attribute
    WHERE  attrelid = :table_name ::regclass
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum
    """
    )
    rs = sesh.execute(stmt, {"table_name": attrelid})
    rv = []
    for name, sql_type in rs:
        rv.append(Column(name=name, type_=ColumnType.from_sql_type(sql_type)))
    return rv


def get_userdata_tableclause(sesh, table_uuid) -> TableClause:
    columns = get_columns(sesh, table_uuid)
    return satable(  # type: ignore
        make_userdata_table_name(table_uuid),
        *[sacolumn(c.name, type_=c.type_.sqla_type()) for c in columns],
        schema="userdata",
    )


def create_table(
    sesh: Session, username: str, table_name: str, columns: Iterable[Column]
) -> UUID:
    table_uuid = uuid4()
    cols: List[SAColumn] = [
        SAColumn("csvbase_row_id", satypes.BigInteger, Identity(), primary_key=True),
        # FIXME: would be good to have these two columns plus
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
        make_userdata_table_name(table_uuid),
        MetaData(bind=engine),
        *cols,
        schema="userdata",
    )
    sesh.execute(CreateTable(table))
    return table_uuid


def delete_table_and_metadata(sesh: Session, username: str, table_name: str) -> None:
    table_model = (
        sesh.query(models.Table)
        .join(models.User)
        .filter(models.Table.table_name == table_name, models.User.username == username)
        .first()
    )
    if table_model is None:
        raise exc.TableDoesNotExistException(username, table_name)
    sa_table = SATable(
        make_userdata_table_name(table_model.table_uuid),
        MetaData(bind=engine),
        schema="userdata",
    )
    sesh.query(models.Praise).filter(
        models.Praise.table_uuid == table_model.table_uuid
    ).delete()
    sesh.delete(table_model)
    sesh.execute(DropTable(sa_table))  # type: ignore


def create_table_metadata(
    sesh: Session,
    table_uuid: UUID,
    user_uuid: UUID,
    table_name: str,
    is_public: bool,
    caption: str,
    licence: DataLicence,
) -> None:
    table_obj = models.Table(
        table_uuid=table_uuid, table_name=table_name, user_uuid=user_uuid
    )
    sesh.add(table_obj)
    table_obj.public = is_public
    table_obj.caption = caption
    table_obj.licence_id = licence.value


def update_table_metadata(
    sesh: Session,
    table_uuid: UUID,
    is_public: bool,
    caption: str,
    licence: DataLicence,
) -> None:
    table_obj = sesh.get(models.Table, table_uuid)  # type: ignore
    table_obj.public = is_public
    table_obj.caption = caption
    table_obj.licence_id = licence.value


def get_readme_markdown(sesh, user_uuid: UUID, table_name: str) -> Optional[str]:
    readme = (
        sesh.query(models.TableReadme.readme_markdown)
        .join(models.Table)
        .filter(
            models.Table.user_uuid == user_uuid, models.Table.table_name == table_name
        )
        .scalar()
    )
    if readme is None:
        return readme
    else:
        return bleach.clean(readme)


def set_readme_markdown(
    sesh, user_uuid: UUID, table_name: str, readme_markdown: str
) -> None:
    # if it's empty or ws-only, don't store it
    if readme_markdown.strip() == "":
        DELETE_STMT = """
        DELETE FROM metadata.table_readmes as tr
        USING metadata.tables as t
        WHERE tr.table_uuid = t.table_uuid
        AND t.table_name = :table_name
        AND t.user_uuid = :user_uuid
        """
        sesh.execute(DELETE_STMT, dict(table_name=table_name, user_uuid=user_uuid))
        logger.info("deleted readme for %s/%s", user_uuid, table_name)
    else:
        table = (
            sesh.query(models.Table)
            .filter(
                models.Table.table_name == table_name,
                models.Table.user_uuid == user_uuid,
            )
            .one()
        )
        if table.readme_obj is None:
            table.readme_obj = models.TableReadme()

        bleached = bleach.clean(readme_markdown)

        table.readme_obj.readme_markdown = bleached


def make_drop_table_ddl(sesh: Session, table_uuid: UUID) -> DropTable:
    sqla_table = get_userdata_tableclause(sesh, table_uuid)
    # sqlalchemy-stubs doesn't match sqla 1.4
    return DropTable(sqla_table)  # type: ignore


def make_truncate_table_ddl(sesh: Session, table_uuid: UUID) -> TextClause:
    return text(f"TRUNCATE {make_userdata_table_name(table_uuid, with_schema=True)}")


def make_userdata_table_name(table_uuid: UUID, with_schema=False):
    # FIXME: This needs to be changed, as we allow usernames and table names up
    # to 200 chars but PG has a limit of 64 chars on table names.  Should
    # probably just be "table_{table_uuid}".  This will also make renaming easier.
    if with_schema:
        return f"userdata.table_{table_uuid.hex}"
    else:
        return f"table_{table_uuid.hex}"


def upsert_table_data(
    sesh: Session,
    user_uuid: UUID,
    username: str,
    table_name: str,
    table_uuid: UUID,
    csv_buf: UserSubmittedCSVData,
    dialect: Type[csv.Dialect],
    columns: Sequence[Column],
    truncate_first=True,
) -> None:
    if truncate_first:
        sesh.execute(make_truncate_table_ddl(sesh, table_uuid))
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
    copy_manager = CopyManager(
        raw_conn, make_userdata_table_name(table_uuid, with_schema=True), cols
    )
    copy_manager.copy(row_gen)


def table_as_csv(
    sesh: Session,
    table_uuid: UUID,
    delimiter: str = ",",
) -> io.StringIO:
    csv_buf = io.StringIO()

    columns = [c.name for c in get_columns(sesh, table_uuid)]

    # this allows for putting the columns in with proper csv escaping
    writer = csv.writer(csv_buf, delimiter=delimiter)
    writer.writerow(columns)

    # FIXME: This is probably too slow
    for row in table_as_rows(sesh, table_uuid):
        writer.writerow(row)

    csv_buf.seek(0)
    return csv_buf


def table_as_xlsx(
    sesh: Session,
    table_uuid: UUID,
    excel_table: bool = False,
) -> io.BytesIO:
    xlsx_buf = io.BytesIO()

    column_names = [c.name for c in get_columns(sesh, table_uuid)]

    rows = table_as_rows(sesh, table_uuid)

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
    table_uuid: UUID,
) -> Iterable[Tuple[PythonType]]:
    table_clause = get_userdata_tableclause(sesh, table_uuid)
    columns = get_columns(sesh, table_uuid)
    q = select([getattr(table_clause.c, c.name) for c in columns]).order_by(
        table_clause.c.csvbase_row_id
    )
    yield from sesh.execute(q)


def table_page(sesh: Session, username: str, table: Table, keyset: KeySet) -> Page:
    """Get a page from a table based on the provided KeySet"""
    # FIXME: this doesn't handle empty tables
    table_clause = get_userdata_tableclause(sesh, table.table_uuid)
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


def get_row(sesh: Session, table_uuid: UUID, row_id: int) -> Optional[Row]:
    columns = get_columns(sesh, table_uuid)
    table_clause = get_userdata_tableclause(sesh, table_uuid)
    cursor = sesh.execute(
        table_clause.select().where(table_clause.c.csvbase_row_id == row_id)
    )
    row = cursor.fetchone()
    if row is None:
        return None
    else:
        return {c: row[c.name] for c in columns}


def get_a_sample_row(sesh: Session, table_uuid: UUID) -> Row:
    """Returns a sample row from the table (the lowest row id).

    If none exist, a made-up row is returned.  This function is for
    example/documentation purposes only."""
    columns = get_columns(sesh, table_uuid)
    table_clause = get_userdata_tableclause(sesh, table_uuid)
    cursor = sesh.execute(table_clause.select().order_by("csvbase_row_id").limit(1))
    row = cursor.fetchone()
    if row is None:
        # return something made-up
        return {c: c.type_.example() for c in columns}
    else:
        return {c: row[c.name] for c in columns}


def get_a_made_up_row(sesh: Session, table_uuid: UUID) -> Row:
    columns = get_columns(sesh, table_uuid)
    return {c: c.type_.example() for c in columns}


def update_row(
    sesh: Session,
    table_uuid: UUID,
    row_id: int,
    row: Row,
) -> bool:
    """Update a given row, returning True if it existed (and was updated) and False otherwise."""
    table = get_userdata_tableclause(sesh, table_uuid)
    values = {c.name: v for c, v in row.items()}
    result = sesh.execute(
        table.update().where(table.c.csvbase_row_id == row_id).values(values)
    )
    return result.rowcount > 0


def delete_row(sesh: Session, table_uuid: UUID, row_id: int) -> bool:
    """Update a given row, returning True if it existed (and was updated) and False otherwise."""
    table = get_userdata_tableclause(sesh, table_uuid)
    result = sesh.execute(table.delete().where(table.c.csvbase_row_id == row_id))
    return result.rowcount > 0


def insert_row(sesh: Session, table_uuid: UUID, row: Row) -> int:
    table = get_userdata_tableclause(sesh, table_uuid)
    values = {c.name: v for c, v in row.items()}
    return sesh.execute(
        table.insert().values(values).returning(table.c.csvbase_row_id)
    ).scalar()


def is_public(sesh: Session, username: str, table_name: str) -> bool:
    # FIXME: This is in preparation for get_table to use memcache
    table = get_table(sesh, username, table_name)
    return table.is_public


def user_exists(sesh: Session, username: str) -> None:
    # FIXME: This function should probably be removed
    user_by_name(sesh, username)


def create_user(
    sesh, crypt_context, username: str, password_plain: str, email: Optional[str] = None
) -> User:
    user_uuid = uuid4()
    check_username_is_allowed(sesh, username)
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


def check_username_is_allowed(sesh: Session, username: str) -> None:
    is_prohibited: bool = sesh.query(
        exists().where(models.ProhibitedUsername.username == username)
    ).scalar()
    if is_prohibited:
        logger.warning("username prohibited: %s", username)
        raise exc.ProhibitedUsernameException()


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
    sesh: Session, username: str, include_private: bool = False
) -> Iterable[Table]:
    rp = (
        sesh.query(models.Table, models.User.username)
        .join(models.User)
        .filter(models.User.username == username)
        .order_by(models.Table.created.desc())
    )
    if not include_private:
        rp = rp.filter(models.Table.public)
    for table_model, username in rp:
        columns = get_columns(sesh, table_model.table_uuid)
        yield _table_model_and_columns_to_table(username, table_model, columns)


def _table_model_and_columns_to_table(
    username: str, table_model: models.Table, columns: Sequence[Column]
) -> Table:
    return Table(
        table_uuid=table_model.table_uuid,
        username=username,
        table_name=table_model.table_name,
        is_public=table_model.public,
        caption=table_model.caption,
        data_licence=DataLicence(table_model.licence_id),
        columns=columns,
        created=table_model.created,
    )


def get_top_n(sesh: Session, n: int = 10) -> Iterable[Table]:
    # FIXME: Put this in a materialized view and refresh every N minutes
    stmt = """
SELECT
    table_uuid,
    username,
    table_name,
    caption,
    licence_id,
    created
FROM
    metadata.tables AS t
    JOIN metadata.users AS u on t.user_uuid = u.user_uuid
    LEFT JOIN metadata.praise USING (table_uuid)
WHERE public
GROUP BY
    table_uuid, username
ORDER BY
    count(praise_id) / extract(epoch FROM now() - created) DESC,
    created DESC
LIMIT :n;
    """
    rp = sesh.execute(stmt, dict(n=n))
    for table_uuid, username, table_name, caption, licence_id, created in rp:
        columns = get_columns(sesh, table_uuid)
        table = Table(
            table_uuid,
            username,
            table_name,
            True,
            caption,
            DataLicence(licence_id),
            columns,
            created,
        )
        yield table


def get_public_table_names(sesh: Session) -> Iterable[Tuple[str, str]]:
    rs = (
        sesh.query(models.User.username, models.Table.table_name)
        .join(models.Table, models.User.user_uuid == models.Table.user_uuid)
        .filter(models.Table.public)
    )
    yield from rs


def praise(
    sesh: Session, owner_username: str, table_name: str, praiser_uuid: UUID
) -> int:
    stmt = """
    INSERT INTO metadata.praise (table_uuid, user_uuid)
    SELECT table_uuid, :praiser_uuid
    FROM metadata.tables
    JOIN metadata.users USING (user_uuid)
    WHERE table_name = :table_name
    AND username = :owner_username
    RETURNING praise_id
    """
    rp = sesh.execute(
        stmt,
        dict(
            owner_username=owner_username,
            table_name=table_name,
            praiser_uuid=praiser_uuid,
        ),
    )
    return rp.scalar()


def is_praised(sesh: Session, user_uuid: UUID, table_uuid: UUID) -> Optional[int]:
    stmt = """
    SELECT praise_id
    FROM metadata.praise
    WHERE user_uuid = :user_uuid
    AND table_uuid = :table_uuid
    """
    rp = sesh.execute(stmt, dict(user_uuid=user_uuid, table_uuid=table_uuid))
    return rp.scalar()


def unpraise(sesh: Session, praise_id: int) -> None:
    rp = sesh.execute(
        "DELETE FROM praise where praise_id = :praise_id", dict(praise_id=praise_id)
    )
    if rp.rowcount != 1:
        logger.error("praise could not be removed: %s", praise_id)
        raise RuntimeError("could not unpraise")


def load_prohibited_usernames(sesh: Session) -> None:
    # inflect is only ever used here
    import inflect

    table = satable(
        "temp_prohibited_usernames",
        *[sacolumn("username", type_=satypes.String)],
    )
    create_table_stmt = """
CREATE TEMP TABLE temp_prohibited_usernames (
    username text
) ON COMMIT DROP;
    """
    remove_stmt = """
DELETE FROM metadata.prohibited_usernames
WHERE username NOT IN (
        SELECT
            username
        FROM
            temp_prohibited_usernames)
RETURNING
    username;
    """
    add_stmt = """
INSERT INTO metadata.prohibited_usernames (username)
SELECT
    username
FROM
    temp_prohibited_usernames
EXCEPT ALL
SELECT
    username
FROM
    metadata.prohibited_usernames
RETURNING
    username;
    """
    with closing(importlib.resources.open_text(data, "prohibited-usernames")) as text_f:
        words = [line.strip() for line in text_f if "#" not in line]
    p = inflect.engine()
    plurals = [p.plural(word) for word in words]
    words.extend(plurals)

    sesh.execute(create_table_stmt)
    sesh.execute(table.insert(), [{"username": word} for word in words])
    removed_rp = sesh.execute(remove_stmt)
    removed = sorted(t[0] for t in removed_rp.fetchall())
    added_rp = sesh.execute(add_stmt)
    added = sorted(t[0] for t in added_rp.fetchall())
    sesh.commit()
    logger.info("removed the following prohibited usernames: %s", removed)
    logger.info("added the following prohibited usernames: %s", added)
