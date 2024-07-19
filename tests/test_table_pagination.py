from csvbase.value_objs import Column, ColumnType, BinaryOp
from csvbase import svc

import pytest

from .utils import create_table


@pytest.fixture()
def user_with_tables(test_user, sesh):
    for n in range(1, 11):
        table_name = f"table-{n}"
        create_table(
            sesh,
            test_user,
            [Column(name="column", type_=ColumnType.TEXT)],
            table_name=table_name,
        )
    sesh.commit()
    return test_user


def test_first_page(sesh, user_with_tables):
    page = svc.table_page(sesh, user_with_tables.user_uuid, user_with_tables, count=2)
    table_names = [t.table_name for t in page.tables]
    assert table_names == ["table-10", "table-9"]
    assert not page.has_prev
    assert page.has_next


def test_second_page(sesh, user_with_tables):
    first_page = svc.table_page(
        sesh, user_with_tables.user_uuid, user_with_tables, count=2
    )
    last_on_first_page = first_page.tables[-1]

    second_page = svc.table_page(
        sesh,
        user_with_tables.user_uuid,
        user_with_tables,
        count=2,
        key=(last_on_first_page.last_changed, last_on_first_page.table_uuid),
    )
    table_names = [t.table_name for t in second_page.tables]
    assert table_names == ["table-8", "table-7"]
    assert second_page.has_prev
    assert second_page.has_next


def test_back_to_first_page(sesh, user_with_tables):
    first_page = svc.table_page(
        sesh, user_with_tables.user_uuid, user_with_tables, count=2
    )
    last_on_first_page = first_page.tables[-1]

    second_page = svc.table_page(
        sesh,
        user_with_tables.user_uuid,
        user_with_tables,
        count=2,
        key=(last_on_first_page.last_changed, last_on_first_page.table_uuid),
    )
    first_on_second_page = second_page.tables[0]

    back_to_first_page = svc.table_page(
        sesh,
        user_with_tables.user_uuid,
        user_with_tables,
        count=2,
        key=(first_on_second_page.last_changed, first_on_second_page.table_uuid),
        op=BinaryOp.GT,
    )

    table_names = [t.table_name for t in back_to_first_page.tables]
    assert table_names == ["table-10", "table-9"]
    assert not back_to_first_page.has_prev
    assert back_to_first_page.has_next


@pytest.mark.xfail(reason="test not implemented")
def test_last_page():
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_backward_paging():
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_pagination_over_the_top():
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_pagination_under_the_bottom():
    assert False


@pytest.mark.xfail(reason="test not implemented")
def test_paging_on_empty_table():
    assert False
