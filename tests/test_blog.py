from unittest.mock import patch
from uuid import UUID
from lxml import etree

import feedparser
import pytest

from csvbase.blog import svc as blog_svc
from csvbase.blog.value_objs import Post

frist_post = Post(
    "frist",
    "Hello, World",
    UUID("edf795a0-93a9-4b5e-962a-c4194e3fddbb"),
    description="The first post",
    draft=False,
    markdown="Hi, so about *csvbase*...",
)


def test_blog_with_no_posts(client):
    with patch.object(blog_svc, "get_posts", return_value=[]):
        resp = client.get("/blog")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header(root)


def test_blog_with_posts(client):
    with patch.object(blog_svc, "get_posts", return_value=[frist_post]):
        resp = client.get("/blog")
    assert resp.status_code == 200
    html_parser = etree.HTMLParser()
    root = etree.fromstring(resp.data, html_parser)
    assert_feed_header(root)


def test_post(client):
    with patch.object(blog_svc, "get_post", return_value=frist_post):
        resp = client.get("/blog/frist")
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


def test_rss(client):
    with patch.object(blog_svc, "get_post", return_value=frist_post):
        resp = client.get("/blog/posts.rss")
    assert resp.status_code == 200
    assert resp.mimetype == "application/rss+xml"

    parsed = feedparser.parse(resp.data)
    assert parsed["feed"]["title"] == "csvbase blog"
    assert parsed["feed"]["link"] == "http://localhost/blog/posts.rss"

    (entry,) = parsed["entries"]
    assert entry["id"] == str(frist_post.uuid)
    assert entry["link"] == "http://localhost/blog/frist"
    assert entry["title"] == frist_post.title
    assert entry["summary"] == frist_post.description
