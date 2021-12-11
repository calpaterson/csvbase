import pytest


@pytest.mark.xfail(strict=True)
def test_csvs_in_and_out(test_user, sesh, client):
    assert False
