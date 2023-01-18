from . import utils


def test_table_view(client, test_user, ten_rows, content_type):
    resp = client.get(
        f"/{test_user.username}/{ten_rows}", headers={"Accept": content_type.value}
    )
    assert resp.status_code == 200


def test_table_view_with_no_rows(sesh, client, test_user, content_type):
    table = utils.create_table(sesh, test_user, [])
    sesh.commit()
    resp = client.get(
        f"/{test_user.username}/{table.table_name}",
        headers={"Accept": content_type.value},
    )
    assert resp.status_code == 200


def test_table_export(ten_rows, test_user, client):
    resp = client.get(f"/{test_user.username}/{ten_rows}/export")
    assert resp.status_code == 200


def test_table_rest_api_docs(ten_rows, test_user, client):
    resp = client.get(f"/{test_user.username}/{ten_rows}/docs")
    assert resp.status_code == 200
