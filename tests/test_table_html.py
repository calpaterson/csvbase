def test_table_view(client, test_user, ten_rows):
    resp = client.get(f"/{test_user.username}/{ten_rows}")
    assert resp.status_code == 200


def test_table_export(ten_rows, test_user, client):
    resp = client.get(f"/{test_user.username}/{ten_rows}/export")
    assert resp.status_code == 200


def test_table_rest_api_docs(ten_rows, test_user, client):
    resp = client.get(f"/{test_user.username}/{ten_rows}/docs")
    assert resp.status_code == 200
