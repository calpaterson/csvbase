from datetime import date
from typing import Sequence, Tuple

from sqlalchemy.orm import Session

from csvbase import exc
from csvbase.svc import get_table, create_table_metadata, user_by_name
from csvbase.config import get_config
from csvbase.userdata import PGUserdataAdapter
from csvbase.value_objs import Column, ColumnType, KeySet, Row, DataLicence, Backend

from .value_objs import Post

BLOG_COLUMNS = [
    Column("title", ColumnType.TEXT),
    Column("uuid", ColumnType.TEXT),
    Column("description", ColumnType.TEXT),
    Column("draft", ColumnType.BOOLEAN),
    Column("markdown", ColumnType.TEXT),
    Column("cover_image_url", ColumnType.TEXT),
    Column("cover_image_alt", ColumnType.TEXT),
    Column("posted", ColumnType.DATE),
]


def post_from_row(row: Row) -> Post:
    kwargs = {}
    for column, val in row.items():
        if column.name == "csvbase_row_id":
            kwargs["id"] = val
            continue
        kwargs[column.name] = val
    return Post(**kwargs)  # type: ignore


def post_to_row(post: Post) -> Row:
    row = {col: getattr(post, col.name) for col in BLOG_COLUMNS}
    row[Column("csvbase_row_id", ColumnType.INTEGER)] = post.id
    return row


def get_blog_ref() -> Tuple[str, str]:
    config = get_config()
    if config.blog_ref is None:
        raise RuntimeError("no blog ref")
    else:
        username, table_name = config.blog_ref.split("/")
        return username, table_name


def get_posts(sesh: Session) -> Sequence[Post]:
    username, table_name = get_blog_ref()
    table = get_table(sesh, username, table_name)
    backend = PGUserdataAdapter(sesh)
    page = backend.table_page(
        table,
        KeySet([Column("csvbase_row_id", ColumnType.INTEGER)], (0,), op="greater_than"),
    )
    posts = []
    for row in page.rows:
        posts.append(post_from_row(row))
    return sorted(posts, key=lambda p: p.posted or date(1970, 1, 1), reverse=True)


def get_post(sesh: Session, post_id: int) -> Post:
    username, table_name = get_blog_ref()
    table = get_table(sesh, username, table_name)
    backend = PGUserdataAdapter(sesh)
    row = backend.get_row(table.table_uuid, post_id)
    if row is None:
        raise exc.RowDoesNotExistException(username, table_name, post_id)
    return post_from_row(row)


def insert_post(sesh: Session, post: Post) -> None:
    username, table_name = get_blog_ref()
    table = get_table(sesh, username, table_name)
    row = post_to_row(post)
    backend = PGUserdataAdapter(sesh)
    backend.insert_row(table.table_uuid, row)


def make_blog_table(sesh: Session) -> None:
    username, table_name = get_blog_ref()
    user = user_by_name(sesh, username)
    table_uuid = create_table_metadata(
        sesh,
        user.user_uuid,
        table_name,
        False,
        "",
        DataLicence.ALL_RIGHTS_RESERVED,
        Backend.POSTGRES,
    )
    backend = PGUserdataAdapter(sesh)
    backend.create_table(table_uuid, BLOG_COLUMNS)
