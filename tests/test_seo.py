def test_robots(client):
    resp = client.get("/robots.txt")
    assert resp.status_code == 200
    assert resp.data == b"Sitemap: http://localhost/sitemap.xml"
    assert resp.headers["Cache-Control"] == "public, max-age=86400"


def test_sitemap(client):
    resp = client.get("/sitemap.xml")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=86400"
