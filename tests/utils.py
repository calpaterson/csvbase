from dataclasses import dataclass, field
from typing import Optional, Iterable
from datetime import datetime
import random
import string
from os import path
from io import StringIO
import re

from sqlalchemy.orm import Session
import pandas as pd
from werkzeug.datastructures import MultiDict

from csvbase.userdata import PGUserdataAdapter
from csvbase import svc
from csvbase.value_objs import (
    DataLicence,
    Table,
    User,
    Column,
    ROW_ID_COLUMN,
    ColumnType,
    Backend,
)
from csvbase.web.billing import svc as billing_svc

from .value_objs import ExtendedUser

test_data_path = path.join(path.dirname(__file__), "test-data")


def random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))


def make_user(sesh: Session, crypt_context) -> ExtendedUser:
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
        timezone=user.timezone,
    )


def subscribe_user(sesh: Session, user: User) -> None:
    """Mark a user has having subscribed."""
    billing_svc.insert_stripe_customer_id(sesh, user.user_uuid, random_string())
    billing_svc.insert_stripe_subscription(
        sesh, user.user_uuid, FakeStripeSubscription()
    )


def get_df_as_csv(client, url: str) -> pd.DataFrame:
    get_resp = client.get(url)
    return pd.read_csv(
        StringIO(get_resp.data.decode("utf-8")), index_col="csvbase_row_id"
    )


def create_table(
    sesh: Session,
    user: User,
    columns: Iterable[Column] = [ROW_ID_COLUMN, Column("a", type_=ColumnType.INTEGER)],
    table_name=None,
    is_public=True,
    caption="",
    licence=DataLicence.ALL_RIGHTS_RESERVED,
) -> Table:
    if table_name is None:
        table_name = random_string()
    table_uuid = svc.create_table_metadata(
        sesh,
        user.user_uuid,
        table_name,
        is_public=is_public,
        caption=caption,
        licence=licence,
        backend=Backend.POSTGRES,
    )
    backend = PGUserdataAdapter(sesh)
    backend.create_table(table_uuid, columns)
    return svc.get_table(sesh, user.username, table_name)


def random_stripe_url(host: str = "stripe.com", path_prefix="/") -> str:
    """Returns realistic (but randomized) stripe urls.

    Useful for asserting that redirects went to the right url.

    """
    return f"https://{host}{path_prefix}/{random_string()}"


@dataclass
class FakeStripeSubscription:
    status: str = "active"
    current_period_end: int = int(datetime(2018, 1, 3).timestamp())
    id: str = field(default_factory=random_string)


@dataclass
class FakeStripeCheckoutSession:
    status: str
    payment_status: str
    customer: Optional[str] = None
    id: str = field(default_factory=random_string)
    url: str = field(
        default_factory=lambda: random_stripe_url(
            host="checkout.stripe.com", path_prefix="/c/pay"
        )
    )
    subscription: FakeStripeSubscription = field(default_factory=FakeStripeSubscription)


@dataclass
class FakeStripePortalSession:
    url: str = field(
        default_factory=lambda: random_stripe_url(
            "billing.stripe.com", path_prefix="/session"
        )
    )
    id: str = field(default_factory=random_string)


ETAG_REGEX = re.compile(r'W/"[A-Za-z0-9\-\._]+\.[A-Za-z0-9\-\._]+"')


def assert_is_valid_etag(etag: str) -> None:
    # asserts that it's a valid weak etag, and that it looks like it includes a
    # signature (csvbase signs etags)
    # FIXME: check the signature
    assert ETAG_REGEX.match(etag), etag


def parse_form(html_str: str, form_name: str) -> MultiDict[str, str]:
    """Helper function for parsing a form out of HTML."""

    html_parser = etree.HTMLParser()
    root = etree.fromstring(html_str, html_parser)
    sel = CSSSelector(f'form[name="{form_name}"]')
    rv = MultiDict()
    (form,) = sel(root)
    breakpoint()
