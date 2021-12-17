import random
import string
from os import path
from datetime import datetime, timezone

from csvbase import svc, models
from .value_objs import ExtendedUser

test_data_path = path.join(path.dirname(__file__), "test-data")


def random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))


def make_user(sesh, crypt_context):
    username = "testuser-" + random_string()
    password = "password"
    user_uuid = svc.create_user(sesh, crypt_context, username, password, email=None)
    api_key = (
        sesh.query(models.APIKey.api_key)
        .filter(models.APIKey.user_uuid == user_uuid)
        .scalar()
    )
    # FIXME: change create_user to return a User, then copy into the below
    return ExtendedUser(
        username=username,
        user_uuid=user_uuid,
        password="password",
        registered=datetime.now(timezone.utc),
        api_key=api_key,
        email=None,
    )
