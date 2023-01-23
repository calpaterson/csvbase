import pytest

from csvbase import svc
from .utils import random_string


def test_registering_no_whence(client):
    username = random_string()
    response = client.post(
        "/register", data=dict(username=username, password="password")
    )
    assert response.status_code == 302
    assert response.headers["Location"] == f"/{username}"


def test_registering_a_username_thats_taken(client, sesh, app):
    username = random_string()

    svc.create_user(sesh, app.config["CRYPT_CONTEXT"], username, "password")
    sesh.commit()

    resp = client.post("/register", data=dict(username=username, password="password"))
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is taken"}


def test_registering_a_username_that_differs_only_by_case(client, sesh, app):
    username = random_string()

    svc.create_user(sesh, app.config["CRYPT_CONTEXT"], username, "password")
    sesh.commit()

    resp = client.post(
        "/register", data=dict(username=username.capitalize(), password="password")
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is taken (in a different case)"}


@pytest.mark.parametrize("banned_username", ["api", "API", "Api"])
def test_registering_a_banned_username(client, banned_username):
    resp = client.post(
        "/register", data=dict(username=banned_username, password="password")
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is not allowed"}
