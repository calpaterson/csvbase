import os
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4
from lxml import etree

import feedparser
import pytest

from csvbase.blog import svc as blog_svc
from csvbase.blog.value_objs import Post
from csvbase.svc import create_table, get_table, create_table_metadata
from csvbase.value_objs import User, Table, Column, ColumnType, DataLicence

from .utils import random_string


def make_post(post_id: int = 1):
    return Post(
        post_id,
        "Hello, World",
        uuid4(),
        description="The first post",
        draft=False,
        markdown="Hi, so about *csvbase*...",
        cover_image_url="http://example.com/some.jpg",
        cover_image_alt="some jpg",
    )


@pytest.fixture(scope="function")
def blog_table(sesh, test_user: User):
    table_name = random_string()
    columns = [
        Column("title", ColumnType.TEXT),
        Column("uuid", ColumnType.TEXT),
        Column("description", ColumnType.TEXT),
        Column("draft", ColumnType.BOOLEAN),
        Column("markdown", ColumnType.TEXT),
        Column("cover_image_url", ColumnType.TEXT),
        Column("cover_image_alt", ColumnType.TEXT),
        Column("posted", ColumnType.DATE),
    ]
    table_uuid = create_table(sesh, test_user.username, table_name, columns)
    create_table_metadata(
        sesh,
        table_uuid,
        test_user.user_uuid,
        table_name,
        False,
        "",
        DataLicence.ALL_RIGHTS_RESERVED,
    )
    table = get_table(sesh, test_user.username, table_name)
    sesh.commit()
    with patch.dict(
        os.environ, {"CSVBASE_BLOG_REF": f"{test_user.username}/{table.table_name}"}
    ):
        yield table


def test_blog_with_no_posts(client, blog_table):
    resp = client.get("/blog")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header(root)


def test_blog_with_posts(sesh, client, blog_table):
    post = make_post()
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get("/blog")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header(root)


def test_post(sesh, client, blog_table):
    post = make_post()
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get("/blog/1")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header(root)


def assert_feed_header(root):
    links = root.find("head").findall("link")
    for link in links:
        if link.attrib["rel"] == "alternate":
            assert link.attrib["type"] == "application/rss+xml"
            assert link.attrib["title"] == "csvbase blog"
            assert link.attrib["href"] == "http://localhost/blog/posts.rss"
            return
    else:
        pytest.fail("rss feed missing from header metadata")


def test_rss(client, sesh, blog_table):
    post = make_post()
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get("/blog/posts.rss")
    assert resp.status_code == 200
    assert resp.mimetype == "application/rss+xml"

    parsed = feedparser.parse(resp.data)
    assert parsed["feed"]["title"] == "csvbase blog"
    assert parsed["feed"]["link"] == "http://localhost/blog/posts.rss"

    (entry,) = parsed["entries"]
    assert entry["id"] == str(post.uuid)
    assert entry["link"] == f"http://localhost/blog/{post.id}"
    assert entry["title"] == post.title
    assert entry["summary"] == post.description
