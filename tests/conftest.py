from logging import DEBUG, basicConfig
from datetime import date
from unittest.mock import patch
from pathlib import Path

import requests_mock
import pytest
from passlib.context import CryptContext
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from aiosmtpd.controller import Controller

from csvbase import svc
from csvbase.web.app import init_app
from csvbase.config import get_config
from csvbase.db import get_db_url
from csvbase.userdata import PGUserdataAdapter
from csvbase.value_objs import Column, ColumnType, Table

from .utils import make_user, create_table, local_only_gitsource


@pytest.fixture(scope="session")
def crypt_context():
    return CryptContext(["plaintext"])


@pytest.fixture(scope="session")
def app(crypt_context):
    # enable the blog (but with a blank table ref!)
    with patch.object(get_config(), "blog_ref", ""):
        a = init_app()
    a.config["TESTING"] = True

    # Set explicitly here to avoid accidentally entering debug mode during
    # tests due to environment variables.
    a.config["DEBUG"] = False

    # Speeds things up considerably when testing
    a.config["CRYPT_CONTEXT"] = crypt_context
    return a


@pytest.fixture(scope="session")
def engine():
    return create_engine(get_db_url(), future=True)


@pytest.fixture(scope="session")
def session_cls(engine):
    return sessionmaker(engine)


@pytest.fixture(scope="session")
def module_sesh(session_cls):
    """A module-level session, used for things that are done once on a session level"""
    with session_cls() as sesh:
        yield sesh


@pytest.fixture(scope="function")
def sesh(session_cls):
    """A function-level session, used for everything else"""
    with session_cls() as sesh_:
        yield sesh_


@pytest.fixture(scope="function")
def test_user(module_sesh, app):
    user = make_user(
        module_sesh,
        app.config["CRYPT_CONTEXT"],
    )
    module_sesh.commit()
    return user


@pytest.fixture(scope="session")
def configure_logging():
    basicConfig(level=DEBUG)


ROMAN_NUMERALS = ["I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX", "X"]


@pytest.fixture(scope="function")
def ten_rows(test_user, sesh) -> Table:
    data = [
        (numeral, (index % 2) == 0, date(2018, 1, index), index + 0.5)
        for index, numeral in enumerate(ROMAN_NUMERALS, start=1)
    ]
    columns = [
        Column(name="roman_numeral", type_=ColumnType.TEXT),
        Column(name="is_even", type_=ColumnType.BOOLEAN),
        Column(name="as_date", type_=ColumnType.DATE),
        Column(name="as_float", type_=ColumnType.FLOAT),
    ]

    table = create_table(sesh, test_user, columns, caption="Roman numerals")
    backend = PGUserdataAdapter(sesh)
    for row in data:
        backend.insert_row(table.table_uuid, dict(zip(columns, row)))
    sesh.commit()
    return table


@pytest.fixture()
def private_table(test_user, module_sesh):
    x_column = Column("x", type_=ColumnType.INTEGER)
    table = create_table(module_sesh, test_user, [x_column], is_public=False)
    backend = PGUserdataAdapter(module_sesh)
    backend.insert_row(table.table_uuid, {x_column: 1})
    module_sesh.commit()
    return table.table_name


@pytest.fixture(scope="session", autouse=True)
def load_prohibited_usernames(module_sesh, app):
    svc.load_prohibited_usernames(module_sesh)
    module_sesh.commit()


@pytest.fixture
def local_repos_path(tmpdir):
    local_repos = Path(tmpdir) / "local-repos"
    local_repos.mkdir()
    with local_only_gitsource(local_repos):
        yield local_repos


@pytest.fixture(scope="function", autouse=True)
def requests_mocker():
    with requests_mock.Mocker() as mocker:
        yield mocker


class StoringHandler:
    def __init__(self):
        self.received = []

    async def handle_DATA(self, server, session, envelope) -> str:
        self.recieved.append(None)
        return "250 Message accepted for delivery"


@pytest.fixture(scope="function")
def mock_smtpd():
    handler = StoringHandler()
    controller = Controller(handler)
    controller.start()
    yield handler
    controller.stop()
