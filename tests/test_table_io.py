from csvbase.value_objs import Column, ColumnType
from csvbase import table_io


def test_scientific_notation_not_put_into_csv():
    columns = [Column("a_float", type_=ColumnType.FLOAT)]
    rows = [[9.999999974e-07]]

    buf = table_io.rows_to_csv(columns, rows)
    csv_str = buf.getvalue()

    assert csv_str == b"a_float\r\n0.000001\r\n"
