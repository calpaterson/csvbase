import os
from typing import Sequence
from uuid import UUID
import zipfile
import marko

from csvbase.value_objs import Row, KeySet
from csvbase.svc import table_page, get_table
from .value_objs import Post


def post_from_row(row: Row) -> Post:
    kwargs = {}
    for column, val in row.items():
        if column.name == "csvbase_row_id":
            continue
        kwargs[column.name] = val
    return Post(**kwargs)  # type: ignore


def get_posts(sesh) -> Sequence[Post]:
    blog_ref = os.environ["CSVBASE_BLOG_REF"]
    username, table_name = blog_ref.split("/")
    table = get_table(sesh, username, table_name)
    page = table_page(sesh, username, table, KeySet(n=0, op="greater_than"))
    posts = []
    for row in page.rows:
        posts.append(post_from_row(row))
    return posts


def get_post(sesh, post_slug: str) -> Post:
    posts = {post.slug: post for post in get_posts(sesh)}
    return posts[post_slug]
