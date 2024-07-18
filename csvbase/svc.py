import binascii
import re
import secrets
from contextlib import closing
from datetime import datetime, timezone, date
from logging import getLogger
from typing import Iterable, Optional, Sequence, Tuple, cast, List
from uuid import UUID, uuid4
from dataclasses import dataclass

import bleach
from sqlalchemy import (
    column as sacolumn,
    func,
    update,
    types as satypes,
    cast as sacast,
    text,
    or_,
    tuple_ as satuple,
)
from sqlalchemy.orm import Session
from sqlalchemy.sql import exists
from sqlalchemy.sql.expression import table as satable
from sqlalchemy.dialects.postgresql import insert as pginsert
import importlib_resources

from . import exc, models, table_io
from .userdata import PGUserdataAdapter
from .value_objs import (
    Column,
    DataLicence,
    Row,
    Table,
    User,
    RowCount,
    Usage,
    Backend,
    GitUpstream,
    UpstreamVersion,
    ContentType,
    BinaryOp,
)
from .constants import MIN_UUID, FAR_FUTURE
from .follow.git import GitSource, get_repo_path
from .repcache import RepCache

logger = getLogger(__name__)

FLOAT_REGEX = re.compile(r"^(\d+\.)|(\.\d+)|(\d+\.\d?)$")

BOOL_REGEX = re.compile("^(yes|no|true|false|y|n|t|f)$", re.I)

ID_REGEX = re.compile(r"^[A-Za-z][-A-Za-z0-9]+$")


def username_exists(sesh: Session, username: str) -> bool:
    """Whether the given username exists."""
    return sesh.query(
        sesh.query(models.User).filter(models.User.username == username).exists()
    ).scalar()


def username_exists_insensitive(sesh: Session, username: str) -> bool:
    """Whether the given username exists.

    Used to give a more helpful error message on registration.

    """
    # FIXME: add a functional index if users tables gets big
    return sesh.query(
        sesh.query(models.User)
        .filter(func.upper(models.User.username) == func.upper(username))
        .exists()
    ).scalar()


def user_by_name(sesh: Session, username: str) -> User:
    # FIXME: This is quite a hot function, needs some caching
    rp = (
        sesh.query(
            models.User.user_uuid,
            models.User.registered,
            models.APIKey.api_key,
            models.UserEmail.email_address,
            models.User.timezone,
            models.User.mailing_list,
        )
        .join(models.APIKey)
        .outerjoin(models.UserEmail)
        .filter(models.User.username == username)
        .one_or_none()
    )
    if rp is None:
        logger.error("unable to find user: '%s'", username)
        raise exc.UserDoesNotExistException(username)
    else:
        user_uuid, registered, api_key, email, timezone, ml = rp
        return User(
            user_uuid=user_uuid,
            username=username,
            registered=registered,
            api_key=api_key,
            email=email,
            timezone=timezone,
            mailing_list=ml,
        )


def user_by_user_uuid(sesh, user_uuid: UUID) -> User:
    # FIXME: Again, quite a hot function, needs some caching
    rp = (
        sesh.query(
            models.User.username,
            models.User.registered,
            models.APIKey.api_key,
            models.UserEmail.email_address,
            models.User.timezone,
            models.User.mailing_list,
        )
        .join(models.APIKey)
        .outerjoin(models.UserEmail)
        .filter(models.User.user_uuid == user_uuid)
        .first()
    )
    if rp is None:
        raise exc.UserDoesNotExistException(str(user_uuid))
    else:
        username, registered, api_key, email, timezone, ml = rp
        return User(
            user_uuid=user_uuid,
            username=username,
            registered=registered,
            api_key=api_key,
            email=email,
            timezone=timezone,
            mailing_list=ml,
        )


def update_user(sesh, new_user: User) -> None:
    current_user = user_by_user_uuid(sesh, new_user.user_uuid)
    update_fields = {"timezone", "mailing_list"}
    update_arg = {
        field: getattr(new_user, field)
        for field in update_fields
        if getattr(new_user, field) != getattr(current_user, field)
    }
    if len(update_arg) > 0:
        sesh.query(models.User).filter(
            models.User.user_uuid == new_user.user_uuid
        ).update(update_arg)
        logger.info("updated %s for %s", update_arg, new_user.username)
    if new_user.email != current_user.email:
        update_user_email(sesh, new_user)


def update_user_email(sesh, user: User) -> None:
    if user.email == "":
        logger.warning("empty string email address")
    # HTML forms submit empty fields as blank strings.
    if user.email is None or "@" not in user.email:
        sesh.query(models.UserEmail).filter(
            models.UserEmail.user_uuid == user.user_uuid
        ).delete()
        logger.info("removed email address for %s", user.username)
    else:
        insert_stmt = pginsert(models.UserEmail).values(
            user_uuid=user.user_uuid, email_address=user.email
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["user_uuid"], set_={"email_address": user.email}
        )
        sesh.execute(upsert_stmt)
        logger.info("updated email address for %s", user.username)


def table_exists(sesh: Session, user_uuid: UUID, table_name: str) -> bool:
    return sesh.query(
        sesh.query(models.Table)
        .filter(
            models.Table.user_uuid == user_uuid, models.Table.table_name == table_name
        )
        .exists()
    ).scalar()


def get_table_by_uuid(sesh: Session, table_uuid: UUID) -> Table:
    pair = (
        sesh.query(models.User.username, models.Table.table_name)
        .join(models.Table)
        .filter(models.Table.table_uuid == table_uuid)
        .first()
    )
    if pair is None:
        raise exc.TableUUIDDoesNotExistException(table_uuid)
    else:
        return get_table(sesh, pair[0], pair[1])


def get_table(sesh: Session, username: str, table_name: str) -> Table:
    # FIXME: this is very hot
    # - should be cached
    # - should be doing a single query
    # o lord give me performance, but not yet
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
    backend = PGUserdataAdapter(sesh)
    columns = backend.get_columns(table_model.table_uuid)
    row_count = backend.count(table_model.table_uuid)
    source: Optional[models.GitUpstream] = (
        sesh.query(models.GitUpstream)
        .filter(models.GitUpstream.table_uuid == table_model.table_uuid)
        .first()
    )
    unique_column_names = (
        sesh.query(func.array_agg(models.UniqueColumn.column_name))
        .filter(models.UniqueColumn.table_uuid == table_model.table_uuid)
        .scalar()
    )
    return _make_table(
        user.username, table_model, columns, row_count, source, unique_column_names
    )


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
    sesh.query(models.TableReadme).filter(
        models.TableReadme.table_uuid == table_model.table_uuid
    ).delete()
    sesh.query(models.Copy).filter(
        or_(
            models.Copy.from_uuid == table_model.table_uuid,
            models.Copy.to_uuid == table_model.table_uuid,
        )
    ).delete()
    sesh.query(models.GitUpstream).filter(
        models.GitUpstream.table_uuid == table_model.table_uuid
    ).delete()
    sesh.query(models.UniqueColumn).filter(
        models.UniqueColumn.table_uuid == table_model.table_uuid
    ).delete()
    sesh.delete(table_model)

    # Make sure the metadata is purged prior to deleting the userdata,
    # otherwise the metadata remains present which can confuse readers (eg the
    # newest page)
    sesh.flush()

    backend = PGUserdataAdapter(sesh)
    backend.drop_table(table_model.table_uuid)


def create_table_metadata(
    sesh: Session,
    user_uuid: UUID,
    table_name: str,
    is_public: bool,
    caption: str,
    licence: DataLicence,
    backend: Backend,
) -> UUID:
    """Creates the metadata structures for a table (but not the table itself) -
    including assigning the table uuid.

    """
    check_table_name_is_allowed(table_name)

    # We check here to ensure that the table does not exist
    if table_exists(sesh, user_uuid, table_name):
        raise exc.TableAlreadyExists()

    table_uuid = uuid4()
    table_obj = models.Table(
        table_uuid=table_uuid,
        table_name=table_name,
        user_uuid=user_uuid,
        backend_id=backend.value,
    )
    table_obj.public = is_public
    table_obj.caption = caption
    table_obj.licence_id = licence.value
    sesh.add(table_obj)
    return table_uuid


def set_key(sesh: Session, table_uuid: UUID, key: Sequence[Column]) -> None:
    for column in key:
        sesh.add(models.UniqueColumn(table_uuid=table_uuid, column_name=column.name))


def get_key(sesh: Session, table_uuid: UUID) -> Sequence[str]:
    rs = sesh.query(models.UniqueColumn.column_name).filter(
        models.UniqueColumn.table_uuid == table_uuid
    )
    return list(t[0] for t in rs)


def create_git_upstream(sesh: Session, table_uuid: UUID, upstream: GitUpstream) -> None:
    sesh.add(
        models.GitUpstream(
            table_uuid=table_uuid,
            last_sha=upstream.last_sha,
            last_modified=upstream.last_modified,
            https_repo_url=upstream.repo_url,
            branch=upstream.branch,
            path=upstream.path,
        )
    )


def update_upstream(sesh: Session, table: Table) -> None:
    """Send the userdata to the upstream (if there is one)."""
    upstream = table.upstream
    if upstream is not None:
        gua = GitSource()
        backend = PGUserdataAdapter(sesh)
        columns = backend.get_columns(table.table_uuid)
        rows = backend.table_as_rows(table.table_uuid)
        buf = table_io.rows_to_csv(columns, rows)
        gua.update(upstream.repo_url, upstream.branch, upstream.path, buf)
        logger.info("updated '%s'", upstream.repo_url)

        repo_path = get_repo_path(upstream.repo_url, upstream.branch)
        version = gua.get_last_version(repo_path, upstream.path)
        set_version(sesh, table.table_uuid, version)


def set_version(sesh: Session, table_uuid: UUID, version: UpstreamVersion) -> None:
    sesh.query(models.GitUpstream).where(  # type: ignore
        models.GitUpstream.table_uuid == table_uuid
    ).update(
        {
            "last_sha": bytes.fromhex(version.version_id),
            "last_modified": version.last_changed,
        }
    )


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


def get_readme_markdown(sesh: Session, table_uuid: UUID) -> Optional[str]:
    readme = (
        sesh.query(models.TableReadme.readme_markdown)
        .filter(models.TableReadme.table_uuid == table_uuid)
        .scalar()
    )
    if readme is None:
        return None
    else:
        return bleach.clean(readme)


def set_readme_markdown(
    sesh, user_uuid: UUID, table_name: str, readme_markdown: str
) -> None:
    # if it's empty or ws-only, don't store it and indeed remove it
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


def get_a_made_up_row(sesh: Session, table_uuid: UUID) -> Row:
    backend = PGUserdataAdapter(sesh)
    columns = backend.get_columns(table_uuid)
    return {c: c.type_.example() for c in columns}


def is_public(sesh: Session, username: str, table_name: str) -> bool:
    # FIXME: This is in preparation for get_table to use memcache
    table = get_table(sesh, username, table_name)
    return table.is_public


def user_exists(sesh: Session, username: str) -> None:
    # FIXME: This function should probably be removed
    user_by_name(sesh, username)


def create_user(
    sesh,
    crypt_context,
    username: str,
    password_plain: str,
    email: Optional[str] = None,
    mailing_list: bool = False,
) -> User:
    user_uuid = uuid4()
    check_username_is_allowed(sesh, username)
    password_hashed = crypt_context.hash(password_plain)
    registered = datetime.now(timezone.utc)
    user = models.User(
        user_uuid=user_uuid,
        username=username,
        password_hash=password_hashed,
        timezone="UTC",
        registered=registered,
        mailing_list=mailing_list,
    )
    # HTML forms submit empty fields as blank strings.
    if email is not None and "@" in email:
        user.email_obj = models.UserEmail(email_address=email)
    user.api_key = models.APIKey(api_key=secrets.token_bytes(16))

    sesh.add(user)

    return User(
        user_uuid=user.user_uuid,
        username=username,
        registered=user.registered,
        api_key=user.api_key.api_key,
        email=email,
        timezone="UTC",
        mailing_list=mailing_list,
    )


def set_password(sesh, crypt_context, user_uuid: UUID, password_plain: str) -> None:
    password_hashed = crypt_context.hash(password_plain)
    sesh.query(models.User).filter(models.User.user_uuid == user_uuid).update(
        {"password_hash": password_hashed}
    )


def check_table_name_is_allowed(table_name: str) -> None:
    too_long = len(table_name) >= 200
    invalid = not ID_REGEX.match(table_name)
    if any([too_long, invalid]):
        logger.warning("table name prohibited: %s", table_name)
        raise exc.InvalidTableNameException()
    # FIXME: it would probably be good to prohibit some table names
    # is_prohibited: bool = sesh.query(
    #     exists().where(models.ProhibitedUsername.username == username.lower())
    # ).scalar()


def check_username_is_allowed(sesh: Session, username: str) -> None:
    too_long = len(username) >= 200
    invalid = not ID_REGEX.match(username)
    if too_long or invalid:
        raise exc.InvalidUsernameNameException()
    is_prohibited: bool = sesh.query(
        exists().where(models.ProhibitedUsername.username == username.lower())
    ).scalar()
    if is_prohibited:
        logger.warning("username prohibited: %s", username)
        raise exc.ProhibitedUsernameException()
    if username_exists(sesh, username):
        raise exc.UsernameAlreadyExistsException(username)
    if username_exists_insensitive(sesh, username):
        raise exc.UsernameAlreadyExistsInDifferentCaseException(username)


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
) -> Iterable[Table]:
    rp = (
        sesh.query(models.Table, models.User.username, models.GitUpstream)
        .join(models.User)
        .outerjoin(models.GitUpstream)
        .filter(models.Table.user_uuid == user_uuid)
        .order_by(models.Table.created.desc())
    )
    if not include_private:
        rp = rp.filter(models.Table.public)
    backend = PGUserdataAdapter(sesh)
    for table_model, username, source in rp:
        columns = backend.get_columns(table_model.table_uuid)
        row_count = backend.count(table_model.table_uuid)
        unique_column_names = (
            sesh.query(func.array_agg(models.UniqueColumn.column_name))
            .filter(models.UniqueColumn.table_uuid == table_model.table_uuid)
            .scalar()
        )
        yield _make_table(
            username, table_model, columns, row_count, source, unique_column_names
        )


@dataclass
class UserTablePage:
    has_more: bool
    has_less: bool
    tables: List[Table]


def table_page(
    sesh: Session,
    user_uuid: UUID,
    viewer: User,
    op: BinaryOp = BinaryOp.LT,
    key: Tuple[datetime, UUID] = (FAR_FUTURE, MIN_UUID),
    count: int = 10,
) -> UserTablePage:
    """Return a page of tables"""
    base_query = (
        sesh.query(models.Table, models.User.username, models.GitUpstream)
        .join(models.User)
        .outerjoin(models.GitUpstream)
        .filter(models.Table.user_uuid == user_uuid)
    )
    if user_uuid != viewer.user_uuid:
        base_query = base_query.filter(models.Table.public)

    table_key = satuple(models.Table.last_changed, models.Table.table_uuid)
    page_key = satuple(*key)

    rp = base_query
    if op is BinaryOp.LT:
        rp = rp.filter(table_key < page_key).order_by(*(t.desc() for t in table_key))
        rp = rp.limit(count)
    elif op is BinaryOp.GT:
        rp = rp.filter(table_key > page_key).order_by(*table_key)
        rp = rp.limit(count)

        # necessary to reverse in subquery to get the tables in the right order
        # FIXME: this is not 2.0 safe:
        # https://docs.sqlalchemy.org/en/20/changelog/migration_20.html#selecting-from-the-query-itself-as-a-subquery-e-g-from-self
        rp = rp.from_self().order_by(table_key.desc())

    backend = PGUserdataAdapter(sesh)
    tables = []
    for table_model, username, source in rp:
        columns = backend.get_columns(table_model.table_uuid)
        row_count = backend.count(table_model.table_uuid)
        unique_column_names = (
            sesh.query(func.array_agg(models.UniqueColumn.column_name))
            .filter(models.UniqueColumn.table_uuid == table_model.table_uuid)
            .scalar()
        )
        tables.append(
            _make_table(
                username, table_model, columns, row_count, source, unique_column_names
            )
        )

    if len(tables) > 1:
        first_table = (tables[0].last_changed, tables[0].table_uuid)
        last_table = (tables[-1].last_changed, tables[-1].table_uuid)
        has_more = sesh.query(
            base_query.filter(table_key < last_table).exists()
        ).scalar()
        has_less = sesh.query(
            base_query.filter(table_key > first_table).exists()
        ).scalar()
    else:
        raise NotImplementedError()

    return UserTablePage(has_more=has_more, has_less=has_less, tables=tables)


def _make_table(
    username: str,
    table_model: models.Table,
    columns: Sequence[Column],
    row_count: RowCount,
    source: Optional[models.GitUpstream],
    unique_column_names: Optional[Sequence[str]],
) -> Table:
    """Make a Table object from inputs"""
    if unique_column_names is not None:
        key = [c for c in columns if c in unique_column_names]
    else:
        key = None
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
        last_changed=table_model.last_changed,
        upstream=_make_source(source),
        key=key,
    )


def _make_source(
    source_model: Optional[models.GitUpstream],
) -> Optional[GitUpstream]:
    if source_model is None:
        return None
    else:
        return GitUpstream(
            last_sha=source_model.last_sha,
            last_modified=source_model.last_modified,
            repo_url=source_model.https_repo_url,
            branch=source_model.branch,
            path=source_model.path,
        )


def get_newest_tables(sesh: Session, n: int = 10) -> Iterable[Table]:
    newest_tables = (
        sesh.query(models.Table.table_name, models.User.username)
        .join(models.User)
        .where(models.Table.public)
        .order_by(models.Table.created.desc())
        .limit(n)
    )
    for table_name, username in newest_tables:
        yield get_table(sesh, username, table_name)


def get_top_n(sesh: Session, n: int = 10) -> Iterable[Table]:
    # FIXME: Put this in a materialized view and refresh every N minutes
    stmt = text(
        """
SELECT
    table_uuid,
    username,
    table_name,
    caption,
    licence_id,
    created,
    last_changed
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
    )
    backend = PGUserdataAdapter(sesh)
    rp = sesh.execute(stmt, dict(n=n))
    for (
        table_uuid,
        username,
        table_name,
        caption,
        licence_id,
        created,
        last_changed,
    ) in rp:
        columns = backend.get_columns(table_uuid)
        unique_column_names = (
            sesh.query(func.array_agg(models.UniqueColumn.column_name))
            .filter(models.UniqueColumn.table_uuid == table_uuid)
            .scalar()
        )
        if unique_column_names is not None:
            key = [c for c in columns if c in unique_column_names]
        else:
            key = None
        table = Table(
            table_uuid,
            username,
            table_name,
            True,
            caption,
            DataLicence(licence_id),
            columns,
            created,
            backend.count(table_uuid),
            last_changed,
            key=key,
        )
        yield table


def get_public_table_names(sesh: Session) -> Iterable[Tuple[str, str, date]]:
    rs = (
        sesh.query(
            models.User.username,
            models.Table.table_name,
            sacast(models.Table.last_changed, satypes.Date),
        )
        .join(models.Table, models.User.user_uuid == models.Table.user_uuid)
        .filter(models.Table.public)
    )
    yield from rs


def praise(
    sesh: Session, owner_username: str, table_name: str, praiser_uuid: UUID
) -> int:
    stmt = text(
        """
    INSERT INTO metadata.praise (table_uuid, user_uuid)
    SELECT table_uuid, :praiser_uuid
    FROM metadata.tables
    JOIN metadata.users USING (user_uuid)
    WHERE table_name = :table_name
    AND username = :owner_username
    RETURNING praise_id
    """
    )
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
    stmt = text(
        """
    SELECT praise_id
    FROM metadata.praise
    WHERE user_uuid = :user_uuid
    AND table_uuid = :table_uuid
    """
    )
    rp = sesh.execute(stmt, dict(user_uuid=user_uuid, table_uuid=table_uuid))
    return rp.scalar()


def unpraise(sesh: Session, praise_id: int) -> None:
    rp = sesh.execute(
        text("DELETE FROM metadata.praise where praise_id = :praise_id"),
        dict(praise_id=praise_id),
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
    create_table_stmt = text(
        """
CREATE TEMP TABLE temp_prohibited_usernames (
    username text
) ON COMMIT DROP;
    """
    )
    remove_stmt = text(
        """
DELETE FROM metadata.prohibited_usernames
WHERE username NOT IN (
        SELECT
            username
        FROM
            temp_prohibited_usernames)
RETURNING
    username;
    """
    )
    add_stmt = text(
        """
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
    )
    with closing(
        importlib_resources.files("csvbase.data")
        .joinpath("prohibited-usernames")
        .open("r")
    ) as text_f:
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


def mark_table_changed(sesh: Session, table_uuid: UUID) -> None:
    sesh.execute(
        update(models.Table)  # type: ignore
        .where(models.Table.table_uuid == table_uuid)
        .values(last_changed=func.now())
    )


def get_usage(sesh: Session, user_uuid: UUID) -> Usage:
    tables_and_public = sesh.query(models.Table.table_uuid, models.Table.public).filter(
        models.Table.user_uuid == user_uuid
    )
    private_bytes = 0
    private_tables = 0
    public_bytes = 0
    public_tables = 0
    backend = PGUserdataAdapter(sesh)
    for table_uuid, is_public in tables_and_public:
        byte_count = backend.byte_count(table_uuid)
        if is_public:
            public_tables += 1
            public_bytes += byte_count
        else:
            private_tables += 1
            private_bytes += byte_count
    return Usage(
        private_bytes=private_bytes,
        private_tables=private_tables,
        public_bytes=public_bytes,
        public_tables=public_tables,
    )


def record_copy(sesh: Session, existing_uuid: UUID, new_uuid: UUID) -> None:
    """Record that a copy has been made of a table (and when).

    Note: not currently shown anywhere.

    """
    sesh.add(models.Copy(from_uuid=existing_uuid, to_uuid=new_uuid))


def git_tables(sesh: Session) -> Iterable[Tuple[Table, GitUpstream]]:
    """Return all external tables that come from git."""
    rows = (
        sesh.query(models.Table, models.User.username, models.GitUpstream)
        .join(models.User, models.User.user_uuid == models.Table.user_uuid)
        .join(
            models.GitUpstream,
            models.Table.table_uuid == models.GitUpstream.table_uuid,
        )
        .where(~models.GitUpstream.https_repo_url.like("https://example.com%"))
    )
    backend = PGUserdataAdapter(sesh)
    for table_model, username, git_upstream_model in rows:
        columns = backend.get_columns(table_model.table_uuid)
        row_count = backend.count(table_model.table_uuid)
        unique_column_names = (
            sesh.query(func.array_agg(models.UniqueColumn.column_name))
            .filter(models.UniqueColumn.table_uuid == table_model.table_uuid)
            .scalar()
        )
        table = _make_table(
            username,
            table_model,
            columns,
            row_count,
            git_upstream_model,
            unique_column_names,
        )
        ext_source = cast(GitUpstream, table.upstream)
        yield (table, ext_source)


def populate_repcache(
    sesh: Session, table_uuid: UUID, content_type: ContentType
) -> None:
    """Populate the repcache for a given table and content type."""
    backend = PGUserdataAdapter(sesh)
    table = get_table_by_uuid(sesh, table_uuid)
    repcache = RepCache(table_uuid, content_type, table.last_changed)
    if repcache.exists():
        logger.info(
            "not populated repcache for '%s'@%s, already present",
            table.ref(),
            table.last_changed,
        )
    with repcache.open(mode="wb") as rep_file:
        columns = backend.get_columns(table.table_uuid)
        rows = backend.table_as_rows(table.table_uuid)
        if content_type is ContentType.PARQUET:
            table_io.rows_to_parquet(columns, rows, rep_file)
        elif content_type is ContentType.JSON_LINES:
            table_io.rows_to_jsonlines(columns, rows, rep_file)
        elif content_type is ContentType.XLSX:
            table_io.rows_to_xlsx(columns, rows, excel_table=False, buf=rep_file)
        else:
            table_io.rows_to_csv(columns, rows, buf=rep_file)
    logger.info("populated repcache for %s@%s", table.ref(), table.last_changed)
