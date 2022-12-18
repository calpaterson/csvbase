import dataclasses
import os
import zipfile
from typing import Sequence
from uuid import UUID

import marko

from csvbase import exc
from csvbase.svc import get_table
from csvbase.userdata import PGUserdataAdapter
from csvbase.value_objs import Column, ColumnType, KeySet, Row

from .value_objs import Post


def post_from_row(row: Row) -> Post:
    kwargs = {}
    for column, val in row.items():
        if column.name == "csvbase_row_id":
            kwargs["id"] = val
            continue
        kwargs[column.name] = val
    return Post(**kwargs)  # type: ignore


def post_to_row(post: Post) -> Row:
    cols = {
        Column("title", ColumnType.TEXT),
        Column("uuid", ColumnType.TEXT),
        Column("description", ColumnType.TEXT),
        Column("draft", ColumnType.BOOLEAN),
        Column("markdown", ColumnType.TEXT),
        Column("cover_image_url", ColumnType.TEXT),
        Column("cover_image_alt", ColumnType.TEXT),
        Column("posted", ColumnType.DATE),
    }
    row = {col: getattr(post, col.name) for col in cols}
    row[Column("csvbase_row_id", ColumnType.INTEGER)] = post.id
    return row


def get_posts(sesh) -> Sequence[Post]:
    blog_ref = os.environ["CSVBASE_BLOG_REF"]
    username, table_name = blog_ref.split("/")
    table = get_table(sesh, username, table_name)
    page = PGUserdataAdapter.table_page(
        sesh, username, table, KeySet(n=0, op="greater_than")
    )
    posts = []
    for row in page.rows:
        posts.append(post_from_row(row))
    return posts


def get_post(sesh, post_id: int) -> Post:
    blog_ref = os.environ["CSVBASE_BLOG_REF"]
    username, table_name = blog_ref.split("/")
    table = get_table(sesh, username, table_name)
    row = PGUserdataAdapter.get_row(sesh, table.table_uuid, post_id)
    if row is None:
        raise exc.RowDoesNotExistException(username, table_name, post_id)
    return post_from_row(row)


def insert_post(sesh, post: Post) -> None:
    blog_ref = os.environ["CSVBASE_BLOG_REF"]
    username, table_name = blog_ref.split("/")
    table = get_table(sesh, username, table_name)
    row = post_to_row(post)
    PGUserdataAdapter.insert_row(sesh, table.table_uuid, row)
