from unittest.mock import patch
from uuid import UUID

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


def test_blog_with_posts(client):
    with patch.object(blog_svc, "get_posts", return_value=[frist_post]):
        resp = client.get("/blog")
        assert resp.status_code == 200


def test_post(client):
    with patch.object(blog_svc, "get_post", return_value=frist_post):
        resp = client.get("/blog/frist")
        assert resp.status_code == 200
