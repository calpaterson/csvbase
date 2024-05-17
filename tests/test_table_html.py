from .utils import current_user


def test_table_export(ten_rows, test_user, client):
    resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/export")
    assert resp.status_code == 200


def test_table_rest_api_docs__public(ten_rows, test_user, client):
    resp = client.get(f"/{test_user.username}/{ten_rows.table_name}/docs")
    assert resp.status_code == 200


def test_table_rest_api_docs__private(private_table, test_user, client):
    with current_user(test_user):
        resp = client.get(f"/{test_user.username}/{private_table}/docs")
    assert resp.status_code == 200
