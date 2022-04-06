from datetime import datetime, timezone
from logging import basicConfig, DEBUG

from sqlalchemy.orm import sessionmaker
import pytest
from passlib.context import CryptContext

from csvbase import web, svc, db, models
from csvbase.value_objs import DataLicence, Column, ColumnType
from .value_objs import ExtendedUser
from .utils import random_string, make_user


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
    with session_cls() as sesh:
        yield sesh


@pytest.fixture(scope="function")
def sesh(session_cls):
    """A function-level session, used for everything else"""
    with session_cls() as sesh_:
        yield sesh_


@pytest.fixture(scope="session")
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
def ten_rows(test_user, sesh):
    table_name = random_string()
    svc.upsert_table_metadata(
        sesh,
        test_user.user_uuid,
        table_name,
        is_public=True,
        description="Roman numerals",
        licence=DataLicence.ALL_RIGHTS_RESERVED,
    )
    svc.create_table(
        sesh,
        test_user.username,
        table_name,
        [Column("roman_numeral", type_=ColumnType.TEXT)],
    )
    for roman_numeral in ROMAN_NUMERALS:
        svc.insert_row(
            sesh, test_user.username, table_name, {"roman_numeral": roman_numeral}
        )
    sesh.commit()
    return table_name


@pytest.fixture(scope="module")
def private_table(test_user, module_sesh):
    table_name = random_string()
    svc.upsert_table_metadata(
        module_sesh,
        test_user.user_uuid,
        table_name,
        is_public=False,
        description="",
        licence=DataLicence.ALL_RIGHTS_RESERVED,
    )
    svc.create_table(
        module_sesh,
        test_user.username,
        table_name,
        [Column("x", type_=ColumnType.INTEGER)],
    )
    svc.insert_row(module_sesh, test_user.username, table_name, {"x": 1})
    module_sesh.commit()
    return table_name
