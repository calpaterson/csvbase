from csvbase.web.main.bp import get_praise_id_if_exists
from .utils import current_user


def test_praise__praise(sesh, client, test_user, ten_rows):
    with current_user(test_user):
        resp = client.post(f"/{test_user.username}/{ten_rows.table_name}/praise")
        assert resp.status_code == 302
        assert (
            resp.headers["Location"] == f"/{test_user.username}/{ten_rows.table_name}"
        )

        praise_id = get_praise_id_if_exists(sesh, ten_rows)
        assert praise_id is not None


def test_praise__unpraise(sesh, client, test_user, ten_rows):
    with current_user(test_user):
        client.post(f"/{test_user.username}/{ten_rows.table_name}/praise")
        praise_id = get_praise_id_if_exists(sesh, ten_rows)

        resp = client.post(
            f"/{test_user.username}/{ten_rows.table_name}/praise",
            data={"praise-id": praise_id},
        )
        assert resp.status_code == 302
        assert (
            resp.headers["Location"] == f"/{test_user.username}/{ten_rows.table_name}"
        )


def test_praise__not_signed_in(client, test_user, ten_rows):
    resp = client.post(f"/{test_user.username}/{ten_rows.table_name}/praise")
    assert resp.status_code == 401
