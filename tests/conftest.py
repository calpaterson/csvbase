from datetime import datetime, timezone
from logging import basicConfig, DEBUG

import pytest
from passlib.context import CryptContext

from csvbase import web, svc
from .value_objs import ExtendedUser
from .utils import random_string


@pytest.fixture(scope="session")
def app():
    a = web.init_app()
    a.config["TESTING"] = True
    # Speeds things up considerably when testing
    a.config["CRYPT_CONTEXT"] = CryptContext(["plaintext"])
    return a


@pytest.fixture(scope="function")
def sesh(client):
    return web.get_sesh()


@pytest.fixture(scope="function")
def test_user(sesh, app):
    username = "testuser-" + random_string()
    password = "password"
    user_uuid = svc.create_user(
        sesh, app.config["CRYPT_CONTEXT"], username, password, email=None
    )
    # FIXME: change create_user to return a User, then copy into the below
    return ExtendedUser(
        username=username,
        user_uuid=user_uuid,
        password="password",
        registered=datetime.now(timezone.utc),
        email=None,
    )


@pytest.fixture(scope="session")
def configure_logging():
    basicConfig(level=DEBUG)
