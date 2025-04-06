from csvbase import auth

import pytest

from . import utils


def test_auth__can_read_own_tables(sesh, test_user, ten_rows):
    with utils.current_user(test_user):
        auth.ensure_table_access(sesh, ten_rows, "read")


def test_auth__can_write_own_tables(sesh, test_user, ten_rows):
    with utils.current_user(test_user):
        auth.ensure_table_access(sesh, ten_rows, "write")


def test_auth__cannot_write_other_peoples_tables(
    sesh, test_user, ten_rows, crypt_context
):
    other_user = utils.make_user(sesh, crypt_context)
    with utils.current_user(other_user):
        with pytest.raises(RuntimeError):
            auth.ensure_table_access(sesh, ten_rows, "write")
