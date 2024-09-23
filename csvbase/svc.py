import binascii
import re
import secrets
from contextlib import closing
from datetime import datetime, timezone, date
from logging import getLogger
from typing import Iterable, Optional, Sequence, Tuple, cast, List, Union
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
    delete,
)
from sqlalchemy.orm import Session, aliased
from sqlalchemy.sql import exists
from sqlalchemy.sql.expression import table as satable, text as satext
from sqlalchemy.dialects.postgresql import insert as pginsert
import importlib_resources
from typing_extensions import Literal

from . import exc, models, table_io
from .userdata import PGUserdataAdapter
from .value_objs import (
    Column,
    Licence,
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
    UserSettings,
)
from .constants import FAR_FUTURE, MAX_UUID
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
            models.User.settings,
        )
        .join(models.APIKey)
        .outerjoin(models.UserEmail)
        .filter(models.User.username == username)
        .one_or_none()
    )
    if rp is None:
        raise exc.UserDoesNotExistException(username)
    else:
        user_uuid, registered, api_key, email, settings = rp
        return User(
            user_uuid=user_uuid,
            username=username,
            registered=registered,
            api_key=api_key,
            email=email,
            settings=UserSettings.from_json(settings),
        )


def user_by_user_uuid(sesh, user_uuid: UUID) -> User:
    # FIXME: Again, quite a hot function, needs some caching
    rp = (
        sesh.query(
            models.User.username,
            models.User.registered,
            models.APIKey.api_key,
            models.UserEmail.email_address,
            models.User.settings,
        )
        .join(models.APIKey)
        .outerjoin(models.UserEmail)
        .filter(models.User.user_uuid == user_uuid)
        .first()
    )
    if rp is None:
        raise exc.UserDoesNotExistException(str(user_uuid))
    else:
        username, registered, api_key, email, settings = rp
        return User(
            user_uuid=user_uuid,
            username=username,
            registered=registered,
            api_key=api_key,
            email=email,
            settings=UserSettings.from_json(settings),
        )


def update_user(sesh, new_user: User) -> None:
    current_user = user_by_user_uuid(sesh, new_user.user_uuid)
    sesh.query(models.User).filter(models.User.user_uuid == new_user.user_uuid).update(
        {"settings": new_user.settings.to_json()}
    )
    logger.info("updated settings for %s", new_user.username)
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
    spdx_id: Optional[str] = (
        sesh.query(models.Licence.spdx_id)
        .join(
            models.TableLicence,
            models.TableLicence.licence_id == models.Licence.licence_id,
        )
        .filter(models.TableLicence.table_uuid == table_model.table_uuid)
        .scalar()
    )
    return _make_table(
        user.username,
        table_model,
        columns,
        row_count,
        source,
        unique_column_names,
        spdx_id,
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
    licence: Optional[Licence],
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
    sesh.add(table_obj)
    if licence is not None:
        licence_obj = (
            sesh.query(models.Licence)
            .filter(models.Licence.spdx_id == licence.spdx_id)
            .one()
        )
        sesh.add(
            models.TableLicence(
                table_uuid=table_uuid, licence_id=licence_obj.licence_id
            )
        )
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
    sesh.query(models.GitUpstream).where(
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
    licence: Optional[Licence],
) -> None:
    # If we have a table uuid, the table must exist
    table_obj = cast(models.Table, sesh.get(models.Table, table_uuid))

    table_obj.public = is_public
    table_obj.caption = caption
    if licence is None:
        sesh.query(models.TableLicence).filter(
            models.TableLicence.table_uuid == table_uuid
        ).delete()
    else:
        licence_obj = (
            sesh.query(models.Licence)
            .filter(models.Licence.spdx_id == licence.spdx_id)
            .one()
        )
        insert_stmt = pginsert(models.TableLicence).values(
            table_uuid=table_uuid, licence_id=licence_obj.licence_id
        )
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["table_uuid"], set_={"licence_id": licence_obj.licence_id}
        )
        sesh.execute(upsert_stmt)


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


def set_readme_markdown(sesh: Session, table_uuid: UUID, readme_markdown: str) -> None:
    # if it's empty or ws-only, don't store it and indeed remove it
    if readme_markdown.strip() == "":
        DELETE_STMT = satext(
            """
        DELETE FROM metadata.table_readmes as tr
        WHERE tr.table_uuid = tr.table_uuid
        """
        )
        sesh.execute(DELETE_STMT, dict(table_uuid=table_uuid))
    else:
        table = (
            sesh.query(models.Table)
            .filter(
                models.Table.table_uuid == table_uuid,
            )
            .one()
        )
        if table.readme_obj is None:
            table.readme_obj = models.TableReadme()

        bleached = bleach.clean(readme_markdown)

        table.readme_obj.readme_markdown = bleached


def get_user_bio_markdown(sesh: Session, user_uuid: UUID) -> Optional[str]:
    bio = (
        sesh.query(models.UserBio.user_bio_markdown)
        .filter(models.UserBio.user_uuid == user_uuid)
        .scalar()
    )
    if bio is None:
        return None
    else:
        return bleach.clean(bio)


def set_user_bio_markdown(sesh: Session, user_uuid: UUID, bio_markdown: str) -> None:
    # as with readmes, if blank remove
    if bio_markdown.strip() == "":
        DELETE_STMT = satext(
            """
        DELETE FROM metadata.user_bios as ub
        WHERE ub.user_uuid = :user_uuid
        """
        )
        sesh.execute(DELETE_STMT, dict(user_uuid=user_uuid))
        logger.info("deleted user bio for %s", user_uuid)
    else:
        user_bio_obj = (
            sesh.query(models.UserBio)
            .filter(
                models.UserBio.user_uuid == user_uuid,
            )
            .first()
        )
        if user_bio_obj is None:
            user_bio_obj = models.UserBio(user_uuid=user_uuid)
            sesh.add(user_bio_obj)

        bleached = bleach.clean(bio_markdown)

        user_bio_obj.user_bio_markdown = bleached


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
        registered=registered,
        settings={"timezone": "UTC", "mailing_list": mailing_list},
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
        settings=UserSettings(timezone="UTC", mailing_list=mailing_list),
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
    """Return True if the provided api key is valid for that user, False otherwise."""

    # Strip whitespace here to improve usability.
    #
    # API keys do not contain whitespace.  People often copy their key straight
    # from the website into an obscured password input field.  If whitepace got
    # added somewhere along the way (common when copying+pasting from websites)
    # then they end up very confused.
    hex_api_key = hex_api_key.strip()

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


@dataclass
class UserTablePage:
    has_next: bool
    has_prev: bool
    tables: List[Table]


def table_page(
    sesh: Session,
    user_uuid: UUID,
    viewer: Optional[User],
    op: Union[Literal[BinaryOp.LT], Literal[BinaryOp.GT]] = BinaryOp.LT,
    key: Tuple[datetime, UUID] = (FAR_FUTURE, MAX_UUID),
    count: int = 10,
) -> UserTablePage:
    """Return a page of tables for the given user, as should be seen by the
    perspective of the given viewer.

    """
    # NOTE: This function is likely to expand into a kind of mini-orm for
    # querying tables, so will likely include search, ordering, etc

    # all tables by the given user_uuid that viewer should be able to see
    visible_tables = (
        sesh.query(models.Table, models.User.username, models.GitUpstream)
        .join(models.User)
        .outerjoin(models.GitUpstream)
        .filter(models.Table.user_uuid == user_uuid)
    )
    if viewer is None or user_uuid != viewer.user_uuid:
        visible_tables = visible_tables.filter(models.Table.public)

    table_key = satuple(models.Table.last_changed, models.Table.table_uuid)
    page_key = satuple(*key)  # type: ignore
    page_of_tables = visible_tables
    if op is BinaryOp.LT:
        page_of_tables = page_of_tables.filter(table_key < page_key).order_by(
            table_key.desc()
        )
        page_of_tables = page_of_tables.limit(count)
    elif op is BinaryOp.GT:
        page_of_tables = page_of_tables.filter(table_key > page_key).order_by(table_key)
        page_of_tables = page_of_tables.limit(count)

        # necessary to reverse in subquery to get the tables in the right
        # order.  doing this in sqlalchemy 2.0 is quite painful
        subq = page_of_tables.subquery()
        t_a = aliased(models.Table, subq)
        u_a = aliased(models.User, subq)
        gu_a = aliased(models.GitUpstream, subq)
        tk_a = satuple(t_a.last_changed, t_a.table_uuid)
        page_of_tables = sesh.query(t_a, u_a.username, gu_a).order_by(tk_a.desc())

    backend = PGUserdataAdapter(sesh)
    tables = []
    for table_model, username, source in page_of_tables:
        columns = backend.get_columns(table_model.table_uuid)
        row_count = backend.count(table_model.table_uuid)
        unique_column_names = (
            sesh.query(func.array_agg(models.UniqueColumn.column_name))
            .filter(models.UniqueColumn.table_uuid == table_model.table_uuid)
            .scalar()
        )
        spdx_id: Optional[str] = (
            sesh.query(models.Licence.spdx_id)
            .join(
                models.TableLicence,
                models.TableLicence.licence_id == models.Licence.licence_id,
            )
            .filter(models.TableLicence.table_uuid == table_model.table_uuid)
            .scalar()
        )
        tables.append(
            _make_table(
                username,
                table_model,
                columns,
                row_count,
                source,
                unique_column_names,
                spdx_id,
            )
        )

    if len(tables) > 0:
        first_table = (tables[0].last_changed, tables[0].table_uuid)
        last_table = (tables[-1].last_changed, tables[-1].table_uuid)
        has_next = sesh.query(
            visible_tables.filter(table_key < last_table).exists()
        ).scalar()
        has_prev = sesh.query(
            visible_tables.filter(table_key > first_table).exists()
        ).scalar()
    else:
        has_next = has_prev = False

    return UserTablePage(has_next=has_next, has_prev=has_prev, tables=tables)


def _make_table(
    username: str,
    table_model: models.Table,
    columns: Sequence[Column],
    row_count: RowCount,
    source: Optional[models.GitUpstream],
    unique_column_names: Optional[Sequence[str]],
    spdx_id: Optional[str],
) -> Table:
    """Make a Table object from inputs"""
    if unique_column_names is not None:
        key = [c for c in columns if c in unique_column_names]
    else:
        key = None
    licence = Licence.from_spdx_id(spdx_id) if spdx_id is not None else None
    return Table(
        table_uuid=table_model.table_uuid,
        username=username,
        table_name=table_model.table_name,
        is_public=table_model.public,
        caption=table_model.caption,
        columns=columns,
        created=table_model.created,
        row_count=row_count,
        last_changed=table_model.last_changed,
        upstream=_make_source(source),
        key=key,
        licence=licence,
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
    created,
    last_changed,
    spdx_id
FROM
    metadata.tables AS t
    JOIN metadata.users AS u on t.user_uuid = u.user_uuid
    LEFT JOIN metadata.table_licences AS tl using (table_uuid)
    LEFT JOIN metadata.licences AS l ON tl.licence_id = l.licence_id
    LEFT JOIN metadata.praise USING (table_uuid)
WHERE public
GROUP BY
    table_uuid, username, spdx_id
ORDER BY
    count(praise_id) / extract(epoch FROM now() - created) DESC,
    created DESC
LIMIT :n;
    """
    )
    backend = PGUserdataAdapter(sesh)
    rp = sesh.execute(stmt, dict(n=n))
    for table_uuid, username, table_name, caption, created, last_changed, spdx_id in rp:
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
        licence = Licence.from_spdx_id(spdx_id) if spdx_id is not None else None
        table = Table(
            table_uuid,
            username,
            table_name,
            True,
            caption,
            columns,
            created,
            backend.count(table_uuid),
            last_changed,
            key=key,
            licence=licence,
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
    return cast(int, rp.scalar())


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
    stmt = delete(models.Praise).where(models.Praise.praise_id == praise_id)
    rp = sesh.execute(stmt)
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
        update(models.Table)
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
        sesh.query(
            models.Table,
            models.User.username,
            models.GitUpstream,
        )
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
        spdx_id: Optional[str] = (
            sesh.query(models.Licence.spdx_id)
            .join(
                models.TableLicence,
                models.TableLicence.licence_id == models.Licence.licence_id,
            )
            .filter(models.TableLicence.table_uuid == table_model.table_uuid)
            .scalar()
        )
        table = _make_table(
            username,
            table_model,
            columns,
            row_count,
            git_upstream_model,
            unique_column_names,
            spdx_id,
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
            if table.row_count.is_big():
                other_content_types = [
                    ContentType.PARQUET,
                    ContentType.JSON_LINES,
                    ContentType.CSV,
                    ContentType.JSON,
                    ContentType.HTML,
                ]
                raise exc.TooBigForContentType(other_content_types)
            table_io.rows_to_xlsx(
                columns,
                rows,
                excel_table=False,
                buf=rep_file,
                sheet_name=table_io.make_xlsx_sheet_name(table),
            )
        else:
            table_io.rows_to_csv(columns, rows, buf=rep_file)
    logger.info("populated repcache for %s@%s", table.ref(), table.last_changed)
