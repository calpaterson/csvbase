from os import path
from io import StringIO

import pandas as pd
from pandas.testing import assert_frame_equal
from flask import url_for
import pytest

from .utils import test_data_path, random_string


def get_df(client, url) -> pd.DataFrame:
    get_resp = client.get(url)
    return pd.read_csv(
        StringIO(get_resp.data.decode("utf-8")), index_col="csvbase_row_id"
    )


def put_df(client, user, url, df: pd.DataFrame) -> None:
    buf = StringIO()
    df.to_csv(buf)

    post_resp = client.put(
        url,
        data=buf.getvalue(),
        headers={
            "Content-Type": "text/csv",
            "Authorization": user.basic_auth(),
        },
    )
    assert post_resp.status_code == 200



def test_csvs_in_and_out(test_user, sesh, client, ten_rows):
    """Test that a csv can be pulled out, edited, and then posted back"""
    url = f"/{test_user.username}/{ten_rows}"

    df = get_df(client, url)
    df.drop(labels=2, axis="index", inplace=True)  # removed data
    df.loc[11] = "XI"  # added data
    df.loc[5] = "FIVE"  # changed data

    buf = StringIO()
    df.to_csv(buf)

    put_df(client, test_user, url, df)
    second_df = get_df(client, url)

    assert_frame_equal(df, second_df)


@pytest.mark.xfail(reason="not implemented")
def test_putting_a_table_doesnt_break_adding_new_rows():
    # At the moment if you add a new row above the sequence, adding a row 500's.  Some sample code here:
    # https://stackoverflow.com/questions/244243/how-to-reset-postgres-primary-key-sequence-when-it-falls-out-of-sync
    assert False


def test_putting_blanks_makes_them_get_autofilled(test_user, sesh, client, ten_rows):
    url = f"/{test_user.username}/{ten_rows}"

    df = get_df(client, url).reset_index()
    df = pd.concat([df, pd.DataFrame({"roman_numeral": ["XI"]})])
    df = df.set_index("csvbase_row_id")
    df.index = df.index.astype(pd.Int64Dtype())

    put_df(client, test_user, url, df)

    second_df = get_df(client, url)

    # the next number should be 11
    df = df.rename(index={pd.NA: 11})
    assert_frame_equal(df, second_df)
