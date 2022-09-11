from unittest.mock import patch

from csvbase.blog import svc as blog_svc


def test_blog_with_no_posts(client):
    with patch.object(blog_svc, "get_posts", return_value=[]):
        resp = client.get("/blog")
        assert resp.status_code == 200
