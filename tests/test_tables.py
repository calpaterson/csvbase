from os import path
from io import StringIO

import pandas
from flask import url_for
import pytest

from .utils import test_data_path, random_string


def test_csvs_in_and_out(test_user, sesh, client):
    table_name = random_string()
    url = url_for(
        "csvbase.upsert_table", username=test_user.username, table_name=table_name
    )

    with open(path.join(test_data_path, "WID.csv")) as csv:
        put_resp = client.put(
            url,
            data=csv,
            headers={
                "Content-Type": "text/csv",
                "Authorization": test_user.basic_auth(),
            },
        )
    assert put_resp.status_code == 200

    get_resp = client.get(url)
    buf = StringIO(get_resp.data.decode("utf-8"))
    df = pandas.read_csv(buf)
