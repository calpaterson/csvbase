def test_cache_headers_for_landing(client):
    resp = client.get("/")
    assert resp.headers["Cache-Control"] == "no-store"
