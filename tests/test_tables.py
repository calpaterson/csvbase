from os import path
from io import StringIO

import pandas
from pandas.testing import assert_frame_equal
from flask import url_for
import pytest

from .utils import test_data_path, random_string


@pytest.mark.xfail(reason="putting a whole csv is no longer enough")
def test_csvs_in_and_out(test_user, sesh, client):
    table_name = random_string()
    url = url_for(
        "csvbase.upsert_table", username=test_user.username, table_name=table_name
    )

    with open(path.join(test_data_path, "WID.csv")) as wid_csv:
        put_resp = client.put(
            url,
            data=wid_csv,
            headers={
                "Content-Type": "text/csv",
                "Authorization": test_user.basic_auth(),
            },
        )
    assert put_resp.status_code == 200, put_resp.data

    get_resp = client.get(url)
    buf = StringIO(get_resp.data.decode("utf-8"))
    out_df = pandas.read_csv(buf)

    with open(path.join(test_data_path, "WID.csv")) as wid_csv:
        in_df = pandas.read_csv(wid_csv)

    assert_frame_equal(in_df, out_df.drop("csvbase_row_id", axis=1))
