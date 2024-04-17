from csvbase.value_objs import Column, ColumnType
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
