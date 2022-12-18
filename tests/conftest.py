import os
from logging import DEBUG, basicConfig
from unittest.mock import patch

import pytest
from passlib.context import CryptContext
from sqlalchemy.orm import sessionmaker

from csvbase import db, svc, web
from csvbase.userdata import PGUserdataAdapter
from csvbase.value_objs import Column, ColumnType, DataLicence

from .utils import make_user, random_string


@pytest.fixture(scope="session")
def app():
    # enable the blog (but with a blank table ref!)
    with patch.dict(os.environ, {"CSVBASE_BLOG_REF": ""}):
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
    table_uuid = PGUserdataAdapter.create_table(
        sesh,
        test_user.username,
        table_name,
        [Column("roman_numeral", type_=ColumnType.TEXT)],
    )
    svc.create_table_metadata(
        sesh,
        table_uuid,
        test_user.user_uuid,
        table_name,
        is_public=True,
        caption="Roman numerals",
        licence=DataLicence.ALL_RIGHTS_RESERVED,
    )
    column = Column(name="roman_numeral", type_=ColumnType.TEXT)
    for roman_numeral in ROMAN_NUMERALS:
        PGUserdataAdapter.insert_row(sesh, table_uuid, {column: roman_numeral})
    sesh.commit()
    return table_name


@pytest.fixture(scope="module")
def private_table(test_user, module_sesh):
    table_name = random_string()
    x_column = Column("x", type_=ColumnType.INTEGER)
    table_uuid = PGUserdataAdapter.create_table(
        module_sesh,
        test_user.username,
        table_name,
        [x_column],
    )
    svc.create_table_metadata(
        module_sesh,
        table_uuid,
        test_user.user_uuid,
        table_name,
        is_public=False,
        caption="",
        licence=DataLicence.ALL_RIGHTS_RESERVED,
    )
    PGUserdataAdapter.insert_row(module_sesh, table_uuid, {x_column: 1})
    module_sesh.commit()
    return table_name
