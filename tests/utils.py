from dataclasses import dataclass, field
from typing import Optional, Iterable, Mapping, Generator
from datetime import datetime
import random
import string
from os import path
from io import StringIO
import re
import contextlib
from unittest import mock

from lxml import etree
from lxml.cssselect import CSSSelector

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
from csvbase.web import func

from .value_objs import ExtendedUser

test_data_path = path.join(path.dirname(__file__), "test-data")


def random_string() -> str:
    return "".join(random.choice(string.ascii_lowercase) for _ in range(32))


def random_integer() -> int:
    # FIXME: possibly this should be a wider range
    return random.randint(-100, 100)


def random_df() -> pd.DataFrame:
    return pd.DataFrame(
        dict(
            letter=list(string.ascii_lowercase),
            text=[random_string() for _ in range(26)],
            integer=[random_integer() for _ in range(26)],
        )
    )


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
        mailing_list=user.mailing_list,
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


class Form(MultiDict):
    """Basic wrapper for a multidict that let's us transmit other key bits of
    form HTML data.

    """

    def __init__(
        self,
        action: Optional[str],
        method: Optional[str],
        mapping: Optional[Mapping[str, str]] = None,
    ) -> None:
        self.action = action
        self.method = method
        super().__init__(mapping=mapping)


def parse_form(html_str: str) -> Form:
    """Parses a form out of HTML."""

    html_parser = etree.HTMLParser()
    root = etree.fromstring(html_str, html_parser)
    sel = CSSSelector("form")
    forms = sel(root)
    assert len(forms) == 1, "did not find exactly one form"

    (form,) = forms
    input_sel = CSSSelector("input")
    input_elements = input_sel(form)

    rv = Form(form.attrib.get("action", None), form.attrib.get("method", None))
    for input_element in input_elements:
        attrs = input_element.attrib
        if "name" in attrs:
            if attrs.get("type") == "checkbox":
                is_checked = bool(attrs.get("checked", False))
                if is_checked:
                    rv.add(attrs["name"], attrs.get("value", "on"))

            else:
                # "" emulates what the server gets
                rv.add(attrs["name"], attrs.get("value", ""))

    select_sel = CSSSelector("select")
    select_elements = select_sel(form)
    for select_element in select_elements:
        name = select_element.attrib["name"]
        for child in select_element.getchildren():
            if child.attrib.get("selected", None) == "selected":
                value = child.attrib["value"]
                rv[name] = value
                break
    return rv


@contextlib.contextmanager
def current_user(user: User) -> Generator[None, None, None]:
    """Context manager to set the current user (as seen by the web app) to the
    passed user."""
    # Originally the current user was set on flask.g but that caused problems
    # because flask.g is attached to the _app_ context (and not the request
    # context) which is ordinarily popped between requests but is not when
    # testing.
    with mock.patch.object(func, "_get_current_user_inner", return_value=user):
        yield
