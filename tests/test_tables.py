import pytest


@pytest.mark.xfail(strict=True)
def test_nothing():
    assert False
