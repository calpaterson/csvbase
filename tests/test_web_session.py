"""Tests for authentication (the "web session")."""

from base64 import b64encode

import pytest

from csvbase import svc
from csvbase.value_objs import ContentType
from .utils import random_string, current_user, mock_turnstile


def test_registering_no_whence(client, requests_mocker):
    username = random_string()
    mock_turnstile(requests_mocker)
    response = client.post(
        "/register",
        data={
            "username": username,
            "password": "password",
            "email": "",
            "cf-turnstile-response": random_string(),
        },
    )
    assert response.status_code == 302
    assert response.headers["Location"] == f"/{username}"

    get_resp = client.get(f"/{username}", headers={"Accept": ContentType.HTML.value})
    assert get_resp.status_code == 200


def test_registering_a_username_thats_taken(client, sesh, app, requests_mocker):
    username = random_string()

    svc.create_user(sesh, app.config["CRYPT_CONTEXT"], username, "password")
    sesh.commit()

    mock_turnstile(requests_mocker)
    resp = client.post(
        "/register",
        data={
            "username": username,
            "password": "password",
            "cf-turnstile-response": random_string(),
        },
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is taken"}


def test_registering_a_username_that_differs_only_by_case(
    client, sesh, app, requests_mocker
):
    username = random_string()

    svc.create_user(sesh, app.config["CRYPT_CONTEXT"], username, "password")
    sesh.commit()

    mock_turnstile(requests_mocker)
    resp = client.post(
        "/register",
        data={
            "username": username.capitalize(),
            "password": "password",
            "cf-turnstile-response": random_string(),
        },
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is taken (in a different case)"}


@pytest.mark.parametrize("banned_username", ["api", "API", "Api"])
def test_registering_a_banned_username(client, banned_username, requests_mocker):
    mock_turnstile(requests_mocker)
    resp = client.post(
        "/register",
        data={
            "username": banned_username,
            "password": "password",
            "cf-turnstile-response": random_string(),
        },
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
def test_registering_an_invalid_username(client, invalid_username, requests_mocker):
    mock_turnstile(requests_mocker)
    resp = client.post(
        "/register",
        data={
            "username": invalid_username,
            "password": "password",
            "cf-turnstile-response": random_string(),
        },
    )
    assert resp.status_code == 400, resp.data
    assert resp.json == {"error": "that username is invalid"}


def test_going_to_registration_form_when_signed_in(client, test_user):
    with current_user(test_user):
        resp = client.get("/register")
        assert resp.status_code == 302
        assert resp.headers["Location"] == f"/{test_user.username}"


@pytest.mark.parametrize("whence", [None, "/about"])
def test_sign_out(client, test_user, whence):
    with current_user(test_user):
        headers = {} if whence is None else {"Referer": whence}
        resp = client.get("/sign-out", headers=headers)
        assert resp.status_code == 302
        if whence is None:
            expected_location = "/"
        else:
            expected_location = whence
        assert expected_location == resp.headers["Location"]
        assert resp.headers["Clear-Site-Data"] == "*"


def test_api_key__invalid(client, test_user, ten_rows):
    """Test for an incorrectly formatted api key (most often this is a user's own password)."""
    encoded = b64encode(f"{test_user.username}:password".encode("utf-8")).decode(
        "utf-8"
    )
    authorization = f"Basic {encoded}"
    resp = client.get(
        f"{test_user.username}/{ten_rows.table_name}",
        headers={"Authorization": authorization},
    )
    assert resp.status_code == 400
    assert resp.json == {"error": "invalid api key"}


def test_api_key__with_whitespace(client, test_user, ten_rows):
    encoded = b64encode(
        f"{test_user.username}: {test_user.hex_api_key()}".encode("utf-8")
    ).decode("utf-8")
    authorization = f"Basic {encoded}"
    resp = client.get(
        f"{test_user.username}/{ten_rows.table_name}",
        headers={"Authorization": authorization},
    )
    assert resp.status_code == 200
