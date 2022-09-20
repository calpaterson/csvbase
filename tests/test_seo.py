from lxml import etree


def test_robots(client):
    resp = client.get("/robots.txt")
    assert resp.status_code == 200
    assert resp.data == b"Sitemap: http://localhost/sitemap.xml"
    assert resp.headers["Cache-Control"] == "public, max-age=86400"


def test_sitemap(client):
    resp = client.get("/sitemap.xml")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=86400"

    # Check it's valid XML
    root = etree.XML(resp.data)
    assert root is not None

    # Double check this easy-to-create issue
    first_line = resp.data.splitlines()[0]
    assert first_line == b"<?xml version='1.0' encoding='UTF-8'?>"
