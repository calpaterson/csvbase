"""Tests for usage and quota functionality"""

from csvbase.userdata import PGUserdataAdapter


def test_usage(sesh, test_user, ten_rows):
    byte_count = PGUserdataAdapter.byte_count(sesh, ten_rows.table_uuid)
    assert byte_count >= 1
