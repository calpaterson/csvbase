from io import BytesIO
from datetime import timedelta

import pytest
from csvbase import temp, exc
from .utils import random_string


def test_temp__set_and_get():
    contents = f"{random_string()}\n".encode("utf-8")
    buf = BytesIO(contents)

    file_id = temp.store_temp_file(buf)
    with temp.retrieve_temp_file(file_id) as f:
        assert f.read() == contents


def test_temp__missing():
    with pytest.raises(exc.MissingTempFile):
        with temp.retrieve_temp_file("nonsense") as f:
            f.read()


def test_temp__expiry():
    contents = f"{random_string()}\n".encode("utf-8")
    buf = BytesIO(contents)

    file_id = temp.store_temp_file(buf, duration=timedelta(seconds=-1))
    with pytest.raises(exc.MissingTempFile):
        with temp.retrieve_temp_file(file_id) as f:
            f.read()
