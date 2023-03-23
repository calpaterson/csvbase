from csvbase.web.func import set_current_user
from .utils import make_user


def test_user_view__self(client, test_user, ten_rows):
    set_current_user(test_user)
    resp = client.get(f"/{test_user.username}")
    assert resp.status_code == 200


def test_user_view__while_anon(client, test_user, ten_rows):
    resp = client.get(f"/{test_user.username}")
    assert resp.status_code == 200


def test_user_view__other(app, sesh, client, test_user, ten_rows):
    set_current_user(make_user(sesh, app.config["CRYPT_CONTEXT"]))
    resp = client.get(f"/{test_user.username}")
    assert resp.status_code == 200
