from csvbase.value_objs import Column, ColumnType, ROW_ID_COLUMN
from csvbase.userdata import PGUserdataAdapter

from .utils import create_table


def test_row_id_bounds(sesh, ten_rows):
    backend = PGUserdataAdapter(sesh)
    min_row_id, max_row_id = backend.row_id_bounds(ten_rows.table_uuid)
    assert min_row_id == 1
    assert max_row_id == 10


def test_row_id_bounds_empty_table(sesh, test_user):
    backend = PGUserdataAdapter(sesh)
    empty_table = create_table(sesh, test_user, [Column("a", ColumnType.TEXT)])
    min_row_id, max_row_id = backend.row_id_bounds(empty_table.table_uuid)
    assert min_row_id is None
    assert max_row_id is None


def test_row_id_bounds_negative_row_ids(sesh, test_user):
    backend = PGUserdataAdapter(sesh)
    a_col = Column("a", ColumnType.TEXT)
    csvbase_row_id_col = Column("csvbase_row_id", ColumnType.INTEGER)
    test_table = create_table(sesh, test_user, [a_col])
    backend.insert_row(
        test_table.table_uuid, {csvbase_row_id_col: -1, a_col: "low end"}
    )
    backend.insert_row(
        test_table.table_uuid, {csvbase_row_id_col: 1, a_col: "high end"}
    )
    min_row_id, max_row_id = backend.row_id_bounds(test_table.table_uuid)
    assert min_row_id is -1
    assert max_row_id is 1


def test_upsert__by_csvbase_row_id(sesh, test_user):
    backend = PGUserdataAdapter(sesh)

    n_col = Column("n", ColumnType.INTEGER)
    test_table = create_table(sesh, test_user, [n_col])
    backend.insert_table_data(test_table, [n_col], [[n] for n in range(1, 11)])

    upsert = [
        (1, 1),  # column the same
        (3, 5),  # column changed
        (None, 11),  # new row
    ]  # all other rows deleted implicitly

    backend.upsert_table_data(test_table, (ROW_ID_COLUMN, n_col), upsert)

    end_state = list(backend.table_as_rows(test_table.table_uuid))
    assert end_state == [
        (1, 1),
        (3, 5),
        (11, 11),
    ]
