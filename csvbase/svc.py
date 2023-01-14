import binascii
import csv
import importlib.resources
import io
import json
import re
import secrets
from contextlib import closing
from datetime import datetime, timezone
from logging import getLogger
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Set,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    Type,
    Union,
)
from uuid import UUID, uuid4

import bleach
import pyarrow as pa
import pyarrow.parquet as pq
import xlsxwriter
from pgcopy import CopyManager
from sqlalchemy import column as sacolumn
from sqlalchemy import func
from sqlalchemy import types as satypes
from sqlalchemy.orm import Session
from sqlalchemy.schema import Column as SAColumn
from sqlalchemy.schema import CreateTable, DropTable, MetaData
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql import exists
from sqlalchemy.sql.expression import TableClause, TextClause, select
from sqlalchemy.sql.expression import table as satable
from sqlalchemy.sql.expression import text

from . import conv, data, exc, models
from .userdata import PGUserdataAdapter
from .value_objs import (
    Column,
    ColumnType,
    DataLicence,
    KeySet,
    Page,
    PythonType,
    Row,
    Table,
    User,
    RowCount,
)
from .streams import UserSubmittedCSVData

if TYPE_CHECKING:
    from sqlalchemy.engine import RowProxy

logger = getLogger(__name__)

FLOAT_REGEX = re.compile(r"^(\d+\.)|(\.\d+)|(\d+\.\d?)$")

BOOL_REGEX = re.compile("^(yes|no|true|false|y|n|t|f)$", re.I)


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
        .one_or_none()
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
    columns = PGUserdataAdapter.get_columns(sesh, table_model.table_uuid)
    row_count = PGUserdataAdapter.count(sesh, table_model.table_uuid)
    return _make_table(user.username, table_model, columns, row_count)


def delete_table_and_metadata(sesh: Session, username: str, table_name: str) -> None:
    table_model = (
        sesh.query(models.Table)
        .join(models.User)
        .filter(models.Table.table_name == table_name, models.User.username == username)
        .first()
    )
    if table_model is None:
        raise exc.TableDoesNotExistException(username, table_name)
    sesh.query(models.Praise).filter(
        models.Praise.table_uuid == table_model.table_uuid
    ).delete()
    sesh.delete(table_model)
    PGUserdataAdapter.drop_table(sesh, table_model.table_uuid)


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


def table_as_csv(
    sesh: Session,
    table_uuid: UUID,
    delimiter: str = ",",
) -> io.StringIO:
    csv_buf = io.StringIO()

    columns = [c.name for c in PGUserdataAdapter.get_columns(sesh, table_uuid)]

    # this allows for putting the columns in with proper csv escaping
    writer = csv.writer(csv_buf, delimiter=delimiter)
    writer.writerow(columns)

    # FIXME: This is probably too slow
    for row in PGUserdataAdapter.table_as_rows(sesh, table_uuid):
        writer.writerow(row)

    csv_buf.seek(0)
    return csv_buf


def table_as_jsonlines(sesh: Session, table_uuid: UUID) -> io.StringIO:
    jl_buf = io.StringIO()

    columns = [c.name for c in PGUserdataAdapter.get_columns(sesh, table_uuid)]
    for row in PGUserdataAdapter.table_as_rows(sesh, table_uuid):
        json.dump(dict(zip(columns, row)), jl_buf)
        jl_buf.write("\n")
    jl_buf.seek(0)
    return jl_buf


def table_as_parquet(
    sesh: Session,
    table_uuid: UUID,
) -> io.BytesIO:
    columns = [c.name for c in PGUserdataAdapter.get_columns(sesh, table_uuid)]
    mapping = [
        dict(zip(columns, row))
        for row in PGUserdataAdapter.table_as_rows(sesh, table_uuid)
    ]

    # FIXME: it is extremely annoying that pyarrow.Tables add a numeric index
    # that ends up in the final parquet.  Doesn't look like there is any way to
    # remove this at this point.
    pa_table = pa.Table.from_pylist(mapping)
    parquet_buf = io.BytesIO()
    pq.write_table(pa_table, parquet_buf)
    parquet_buf.seek(0)

    return parquet_buf


def table_as_xlsx(
    sesh: Session,
    table_uuid: UUID,
    excel_table: bool = False,
) -> io.BytesIO:
    xlsx_buf = io.BytesIO()

    column_names = [c.name for c in PGUserdataAdapter.get_columns(sesh, table_uuid)]

    rows = PGUserdataAdapter.table_as_rows(sesh, table_uuid)

    # FIXME: Perhaps this should change based on the user's locale
    workbook_args: Dict = {"default_date_format": "yyyy-mm-dd"}
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


def get_a_made_up_row(sesh: Session, table_uuid: UUID) -> Row:
    columns = PGUserdataAdapter.get_columns(sesh, table_uuid)
    return {c: c.type_.example() for c in columns}


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
        columns = PGUserdataAdapter.get_columns(sesh, table_model.table_uuid)
        row_count = PGUserdataAdapter.count(sesh, table_model.table_uuid)
        yield _make_table(username, table_model, columns, row_count)


def _make_table(
    username: str,
    table_model: models.Table,
    columns: Sequence[Column],
    row_count: RowCount,
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
        row_count=row_count,
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
        columns = PGUserdataAdapter.get_columns(sesh, table_uuid)
        table = Table(
            table_uuid,
            username,
            table_name,
            True,
            caption,
            DataLicence(licence_id),
            columns,
            created,
            PGUserdataAdapter.count(sesh, table_uuid),
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
