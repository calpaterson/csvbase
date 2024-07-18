from csvbase.value_objs import Column, ColumnType
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
    assert not page.has_less
    assert page.has_more


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
    assert second_page.has_less
    assert second_page.has_more
