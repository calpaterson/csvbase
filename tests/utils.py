import random
import string
from os import path
from io import StringIO

import pandas as pd

from csvbase import svc
from .value_objs import ExtendedUser

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
