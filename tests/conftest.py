import string
import random
from datetime import datetime, timezone

import pytest
from passlib.context import CryptContext

from csvbase import web, svc
from .value_objs import ExtendedUser


@pytest.fixture(scope="session")
def app():
    a = web.init_app()
    a.config["TESTING"] = True
    # Speeds things up considerably when testing
    a.config["CRYPT_CONTEXT"] = CryptContext(["plaintext"])
    return a


@pytest.fixture(scope="session")
def sesh(app):
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


def random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))
