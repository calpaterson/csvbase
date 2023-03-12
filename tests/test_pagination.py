import csv
import io
import string

import pytest

from csvbase import svc, streams
from csvbase.userdata import PGUserdataAdapter
from csvbase.value_objs import (
    Column,
    ColumnType,
    DataLicence,
    KeySet,
    Page,
    Table,
)

from .utils import random_string


@pytest.fixture()
def letters_table(test_user, module_sesh) -> Table:
    table_name = random_string()

    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    writer.writerow(["letter"])
    for char in string.ascii_lowercase:
        writer.writerow(char)
    csv_buf.seek(0)

    dialect, columns = streams.peek_csv(csv_buf)
    csv_buf.seek(0)
    table_uuid = PGUserdataAdapter.create_table(module_sesh, columns)
    svc.create_table_metadata(
        module_sesh,
        table_uuid,
        test_user.user_uuid,
        table_name,
        True,
        "",
        DataLicence.ALL_RIGHTS_RESERVED,
    )
    table = svc.get_table(module_sesh, test_user.username, table_name)
    PGUserdataAdapter.insert_table_data(
        module_sesh,
        table,
        csv_buf,
        dialect,
        columns,
    )
    module_sesh.commit()
    return svc.get_table(module_sesh, test_user.username, table_name)


def rows_to_alist(rows):
    return [tuple(row.values()) for row in rows]


csvbase_row_id_col = Column("csvbase_row_id", ColumnType.INTEGER)


def test_first_page(sesh, test_user, letters_table):
    page = PGUserdataAdapter.table_page(
        sesh,
        letters_table,
        keyset=KeySet(
            columns=[csvbase_row_id_col],
            values=(0,),
            op="greater_than",
            size=3,
        ),
    )

    assert page.has_less is False
    assert rows_to_alist(page.rows) == [(1, "a"), (2, "b"), (3, "c")]
    assert page.has_more is True


def test_second_page(sesh, test_user, letters_table):
    page = PGUserdataAdapter.table_page(
        sesh,
        letters_table,
        keyset=KeySet([csvbase_row_id_col], (3,), op="greater_than", size=3),
    )

    assert page.has_less is True
    assert rows_to_alist(page.rows) == [(4, "d"), (5, "e"), (6, "f")]
    assert page.has_more is True


def test_back_to_first_page(sesh, test_user, letters_table):
    page = PGUserdataAdapter.table_page(
        sesh,
        letters_table,
        keyset=KeySet([csvbase_row_id_col], values=(4,), op="less_than", size=3),
    )

    assert not page.has_less
    assert rows_to_alist(page.rows) == [(1, "a"), (2, "b"), (3, "c")]
    assert page.has_more


def test_last_page(sesh, test_user, letters_table):
    page = PGUserdataAdapter.table_page(
        sesh,
        letters_table,
        keyset=KeySet([csvbase_row_id_col], values=(23,), op="greater_than", size=10),
    )

    assert page.has_less is True
    assert rows_to_alist(page.rows) == [(24, "x"), (25, "y"), (26, "z")]
    assert page.has_more is False


def test_backward_paging(sesh, test_user, letters_table):
    page = PGUserdataAdapter.table_page(
        sesh,
        letters_table,
        keyset=KeySet([csvbase_row_id_col], values=(23,), op="less_than", size=3),
    )

    assert page.has_less is True
    assert rows_to_alist(page.rows) == [(20, "t"), (21, "u"), (22, "v")]
    assert page.has_more is True


def test_pagination_over_the_top(sesh, test_user, letters_table):
    page = PGUserdataAdapter.table_page(
        sesh,
        letters_table,
        keyset=KeySet([csvbase_row_id_col], values=(50,), op="greater_than", size=3),
    )
    assert page.has_less
    assert rows_to_alist(page.rows) == []
    assert not page.has_more


def test_pagination_under_the_bottom(sesh, test_user):
    table_name = random_string()
    x_column = Column("x", ColumnType.INTEGER)
    table_uuid = PGUserdataAdapter.create_table(sesh, columns=[x_column])
    svc.create_table_metadata(
        sesh, table_uuid, test_user.user_uuid, table_name, False, "", DataLicence.OGL
    )

    row_ids = [
        PGUserdataAdapter.insert_row(sesh, table_uuid, {x_column: 1}) for _ in range(5)
    ]

    for row_id in row_ids[:3]:
        PGUserdataAdapter.delete_row(sesh, table_uuid, row_id)

    table = svc.get_table(sesh, test_user.username, table_name)

    page = PGUserdataAdapter.table_page(
        sesh,
        table,
        keyset=KeySet([csvbase_row_id_col], (3,), op="less_than", size=3),
    )
    assert page.has_more
    assert rows_to_alist(page.rows) == []
    assert not page.has_less


def test_paging_on_empty_table(sesh, test_user):
    table_name = random_string()

    table_uuid = PGUserdataAdapter.create_table(
        sesh, columns=[Column("x", ColumnType.INTEGER)]
    )
    svc.create_table_metadata(
        sesh, table_uuid, test_user.user_uuid, table_name, False, "", DataLicence.OGL
    )
    table = svc.get_table(sesh, test_user.username, table_name)

    page = PGUserdataAdapter.table_page(
        sesh,
        table,
        keyset=KeySet([csvbase_row_id_col], values=(0,), op="greater_than"),
    )

    assert page == Page(rows=[], has_less=False, has_more=False)
