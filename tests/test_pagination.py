import string
import io
import csv

from csvbase import svc
from csvbase.value_objs import KeySet
from .utils import random_string

import pytest


@pytest.fixture(scope="session")
def letters_table(test_user, module_sesh):
    table_name = random_string()

    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["letter"])
    for char in string.ascii_lowercase:
        writer.writerow(char)
    csv_buf.seek(0)

    svc.upsert_table(
        module_sesh, test_user.user_uuid, test_user.username, table_name, csv_buf
    )
    module_sesh.commit()
    return table_name


def test_no_pagination(sesh, test_user, letters_table):
    page = svc.table_page(
        sesh,
        test_user.user_uuid,
        test_user.username,
        letters_table,
        keyset=KeySet(n=0, op="greater_than", size=3),
    )

    assert page.has_less == False
    assert page.rows == [(1, "a"), (2, "b"), (3, "c")]
    assert page.has_more == True


def test_second_page(sesh, test_user, letters_table):
    page = svc.table_page(
        sesh,
        test_user.user_uuid,
        test_user.username,
        letters_table,
        keyset=KeySet(n=3, op="greater_than", size=3),
    )

    assert page.has_less == True
    assert page.rows == [(4, "d"), (5, "e"), (6, "f")]
    assert page.has_more == True


def test_back_to_first_page(sesh, test_user, letters_table):
    page = svc.table_page(
        sesh,
        test_user.user_uuid,
        test_user.username,
        letters_table,
        keyset=KeySet(n=4, op="less_than", size=3),
    )

    assert page.has_less == False
    assert page.rows == [(1, "a"), (2, "b"), (3, "c")]
    assert page.has_more == True


def test_last_page(sesh, test_user, letters_table):
    page = svc.table_page(
        sesh,
        test_user.user_uuid,
        test_user.username,
        letters_table,
        keyset=KeySet(n=23, op="greater_than", size=10),
    )

    assert page.has_less == True
    assert page.rows == [(24, "x"), (25, "y"), (26, "z")]
    assert page.has_more == False


def test_backward_paging(sesh, test_user, letters_table):
    page = svc.table_page(
        sesh,
        test_user.user_uuid,
        test_user.username,
        letters_table,
        keyset=KeySet(n=23, op="less_than", size=3),
    )

    assert page.has_less == True
    assert page.rows == [(20, "t"), (21, "u"), (22, "v")]
    assert page.has_more == True


@pytest.mark.xfail(reason="empty tables not well supported yet")
def test_paging_on_empty_table(sesh, test_user):
    table_name = random_string()

    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["letter"])
    csv_buf.seek(0)

    svc.upsert_table(sesh, test_user.user_uuid, test_user.username, table_name, csv_buf)

    page = svc.table_page(
        sesh,
        test_user.user_uuid,
        test_user.username,
        table_name,
        keyset=KeySet(n=0, op="greater_than"),
    )
