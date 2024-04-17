"""Tests for usage and quota functionality"""

from csvbase.userdata import PGUserdataAdapter


def test_usage(sesh, test_user, ten_rows):
    backend = PGUserdataAdapter(sesh)
    byte_count = backend.byte_count(ten_rows.table_uuid)
    assert byte_count >= 1
