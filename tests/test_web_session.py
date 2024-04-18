import pytest

from csvbase.web.func import set_current_user
from csvbase import svc
from .utils import random_string


def test_registering_no_whence(client):
    username = random_string()
    response = client.post(
        "/register", data=dict(username=username, password="password", email="")
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


@pytest.mark.parametrize(
    "invalid_username",
    [
        pytest.param("1", id="starts with number(1)"),
        pytest.param("2cool4school", id="starts with number(2)"),
        pytest.param("cool_guy", id="underscores"),
        pytest.param("_leader", id="leading underscore"),
        pytest.param("f" * 300, id="too long"),
    ],
)
def test_registering_an_invalid_username(client, invalid_username):
    resp = client.post(
        "/register", data=dict(username=invalid_username, password="password")
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is invalid"}


def test_going_to_registration_form_when_signed_in(client, test_user):
    set_current_user(test_user)

    resp = client.get("/register")
    assert resp.status_code == 302
    assert resp.headers["Location"] == f"/{test_user.username}"


@pytest.mark.parametrize("whence", [None, "/about"])
def test_sign_out(client, test_user, whence):
    set_current_user(test_user)
    headers = {} if whence is None else {"Referer": whence}
    resp = client.get("/sign-out", headers=headers)
    assert resp.status_code == 302
    if whence is None:
        expected_location = "/"
    else:
        expected_location = whence
    assert expected_location == resp.headers["Location"]
    assert resp.headers["Clear-Site-Data"] == "*"
