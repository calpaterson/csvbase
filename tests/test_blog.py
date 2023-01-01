import os
from pathlib import Path
from unittest.mock import patch
from uuid import UUID, uuid4

import feedparser
import pytest
from lxml import etree

from csvbase.blog import svc as blog_svc
from csvbase.blog.value_objs import Post
from csvbase.svc import create_table_metadata, get_table
from csvbase.userdata import PGUserdataAdapter
from csvbase.value_objs import Column, ColumnType, DataLicence, Table, User

from .utils import random_string


def make_post(**overrides):
    kwargs = {
        "id": 1,
        "title": "Hello, World",
        "uuid": str(uuid4()),
        "description": "The first post",
        "draft": False,
        "markdown": "Hi, so about *csvbase*...",
        "cover_image_url": "http://example.com/some.jpg",
        "cover_image_alt": "some jpg",
    }
    kwargs.update(**overrides)
    return Post(**kwargs)  # type: ignore


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
    table_uuid = PGUserdataAdapter.create_table(sesh, columns)
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
    assert_feed_header_is_present(root)


def test_blog_with_posts(sesh, client, blog_table):
    post1 = make_post()
    post2 = make_post(id=2, draft=True, title="A draft post")
    blog_svc.insert_post(sesh, post1)
    blog_svc.insert_post(sesh, post2)
    sesh.commit()
    resp = client.get("/blog")
    assert resp.status_code == 200
    assert b"A draft post" not in resp.data
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header_is_present(root)


def test_blog_with_draft_post(sesh, client, blog_table):
    post = make_post()
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get("/blog")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header_is_present(root)


def test_post(sesh, client, blog_table):
    post = make_post()
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get("/blog/1")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header_is_present(root)
    # FIXME: It would be great to have tests for all this stuff:
    # image
    # image alt
    # og:url
    #   : site_name
    #   : author
    #   : title
    #   : local
    #   : type (article)
    # article:published_time
    # ld+json
    # twitter:card
    # date
    # description


def test_draft(client, sesh, blog_table):
    post = make_post(draft=True)
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get(f"/blog/{post.id}")
    assert resp.status_code == 404


def test_draft_with_uuid(client, sesh, blog_table):
    post = make_post(draft=True)
    blog_svc.insert_post(sesh, post)
    sesh.commit()
    resp = client.get(f"/blog/{post.id}?uuid={str(post.uuid)}")
    assert resp.status_code == 200


def assert_feed_header_is_present(root):
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
    post1 = make_post()
    post2 = make_post(id=2, draft=True)
    blog_svc.insert_post(sesh, post1)
    blog_svc.insert_post(sesh, post2)
    sesh.commit()
    resp = client.get("/blog/posts.rss")
    assert resp.status_code == 200
    assert resp.mimetype == "application/rss+xml"

    parsed = feedparser.parse(resp.data)
    assert parsed["feed"]["title"] == "csvbase blog"
    assert parsed["feed"]["link"] == "http://localhost/blog/posts.rss"

    (entry,) = parsed["entries"]
    assert entry["id"] == str(post1.uuid)
    assert entry["link"] == f"http://localhost/blog/{post1.id}"
    assert entry["title"] == post1.title
    assert entry["summary"] == post1.description
