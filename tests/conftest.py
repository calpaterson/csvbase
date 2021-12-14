from datetime import datetime, timezone
from logging import basicConfig, DEBUG

from sqlalchemy.orm import sessionmaker
import pytest
from passlib.context import CryptContext

from csvbase import web, svc, db
from .value_objs import ExtendedUser
from .utils import random_string


@pytest.fixture(scope="session")
def app():
    a = web.init_app()
    a.config["TESTING"] = True
    # Speeds things up considerably when testing
    a.config["CRYPT_CONTEXT"] = CryptContext(["plaintext"])
    return a


@pytest.fixture(scope="session")
def session_cls():
    return sessionmaker(bind=db.engine)


@pytest.fixture(scope="session")
def module_sesh(session_cls):
    """A module-level session, used for things that are done once on a session level"""
    return session_cls()


@pytest.fixture(scope="function")
def sesh(session_cls):
    """A function-level session, used for everything else"""
    return session_cls()


@pytest.fixture(scope="session")
def test_user(module_sesh, app):
    username = "testuser-" + random_string()
    password = "password"
    user_uuid = svc.create_user(
        module_sesh, app.config["CRYPT_CONTEXT"], username, password, email=None
    )
    module_sesh.commit()
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
