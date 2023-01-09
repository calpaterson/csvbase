import random
import string
from os import path
from io import StringIO

import pandas as pd

from csvbase.userdata import PGUserdataAdapter
from csvbase import svc
from .value_objs import ExtendedUser
from csvbase.value_objs import DataLicence, Table

test_data_path = path.join(path.dirname(__file__), "test-data")


def random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))


def make_user(sesh, crypt_context):
    username = "testuser-" + random_string()
    password = "password"
    user = svc.create_user(sesh, crypt_context, username, password, email=None)
    return ExtendedUser(
        username=username,
        user_uuid=user.user_uuid,
        password=password,
        registered=user.registered,
        api_key=user.api_key,
        email=user.email,
    )


def get_df_as_csv(client, url) -> pd.DataFrame:
    get_resp = client.get(url)
    return pd.read_csv(
        StringIO(get_resp.data.decode("utf-8")), index_col="csvbase_row_id"
    )


def create_table(
    sesh,
    user,
    columns,
    table_name=None,
    is_public=True,
    caption="",
    licence=DataLicence.ALL_RIGHTS_RESERVED,
) -> Table:
    if table_name is None:
        table_name = random_string()
    table_uuid = PGUserdataAdapter.create_table(sesh, columns)
    svc.create_table_metadata(
        sesh,
        table_uuid,
        user.user_uuid,
        table_name,
        is_public=is_public,
        caption=caption,
        licence=licence,
    )
    return svc.get_table(sesh, user.username, table_name)
