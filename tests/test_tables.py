from os import path
from io import StringIO

import pandas as pd
from pandas.testing import assert_frame_equal
from flask import url_for
import pytest

from .utils import test_data_path, random_string


def test_csvs_in_and_out(test_user, sesh, client, ten_rows):
    """Test that a csv can be pulled out, edited, and then posted back"""
    url = f"/{test_user.username}/{ten_rows}"

    get_resp = client.get(url)
    df = pd.read_csv(
        StringIO(get_resp.data.decode("utf-8")), index_col="csvbase_row_id"
    )

    df.drop(labels=2, axis="index", inplace=True)  # removed data
    df.loc[11] = "XI"  # added data
    df.loc[5] = "FIVE"  # changed data

    buf = StringIO()
    df.to_csv(buf)

    post_resp = client.put(
        url,
        data=buf.getvalue(),
        headers={
            "Content-Type": "text/csv",
            "Authorization": test_user.basic_auth(),
        },
    )
    assert post_resp.status_code == 200

    second_get_resp = client.get(url)
    second_df = pd.read_csv(
        StringIO(second_get_resp.data.decode("utf-8")), index_col="csvbase_row_id"
    )

    assert_frame_equal(df, second_df)
