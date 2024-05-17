import re
import functools
from datetime import datetime, timezone
from typing import Optional, Callable, Mapping, Tuple, Any, Union
from logging import getLogger
from urllib.parse import urlsplit, urlparse
from typing_extensions import Literal

from sqlalchemy.orm import Session

import werkzeug
import werkzeug.exceptions
from werkzeug.wrappers.response import Response
from flask import session as flask_session
from flask import request, current_app, redirect as unsafe_redirect, flash
from flask_babel import get_locale, dates

from .. import exc, sentry, svc
from ..value_objs import User, Table

logger = getLogger(__name__)


def is_browser() -> bool:
    # FIXME: this should call negotiate_content_type
    # bit of content negotiation magic
    accepts = werkzeug.http.parse_accept_header(request.headers.get("Accept"))
    best = accepts.best_match(["text/html", "text/csv"], default="text/csv")
    return best == "text/html"


def set_current_user(user: User) -> None:
    # Previously flask.g was used but that presents issues in testing, where
    # the app context is not popped between requests, making it difficult to
    # test authentication
    request.current_user = user  # type: ignore

    sentry.set_user(user)


def _get_current_user_inner() -> Optional[User]:
    # (This function exists only for mocking/testing purposes.)
    # current_user is put onto request instead of g because tests don't tear
    # down the app context between requests
    if hasattr(request, "current_user"):
        return request.current_user
    else:
        return None


def get_current_user() -> Optional[User]:
    """Return the current user."""
    return _get_current_user_inner()


def get_current_user_or_401() -> User:
    """If there is no current user, raise NotAuthenticatedException.  If the
    user is using a browser, this will redirect to the registration page.

    """
    current_user = get_current_user()
    if current_user is None:
        raise exc.NotAuthenticatedException()
    return current_user


def reverse_url_for(
    url: str, method: str = "GET"
) -> Optional[Tuple[Callable, Mapping]]:
    """Returns the view function that would handle a given url"""
    adapter = current_app.url_map.bind_to_environ(request.environ)
    path = urlsplit(url)[2]
    try:
        view_func, view_args = adapter.match(path, method=method)
    except werkzeug.exceptions.HTTPException:
        logger.warning("'%s' didn't match any routes", url)
        return None
    return current_app.view_functions[view_func], view_args


def user_timezone_or_utc() -> str:
    user = get_current_user()
    if user is not None:
        return user.timezone
    else:
        return "UTC"


# FIXME: upstream this
def format_timedelta(
    datetime_or_timedelta,
    granularity: str = "second",
    add_direction=False,
    threshold=0.85,
):
    """Format the elapsed time from the given date to now or the given
    timedelta.

    This function is also available in the template context as filter
    named `timedeltaformat`.
    """
    if isinstance(datetime_or_timedelta, datetime):
        is_aware = (
            datetime_or_timedelta.tzinfo is not None
            and datetime_or_timedelta.tzinfo.utcoffset(datetime_or_timedelta)
            is not None
        )
        if is_aware:
            now = datetime.now(timezone.utc)
        else:
            now = datetime.utcnow()
        datetime_or_timedelta = now - datetime_or_timedelta
    return dates.format_timedelta(
        datetime_or_timedelta,
        granularity,
        threshold=threshold,
        add_direction=add_direction,
        locale=get_locale(),
    )


_URL_REGEX = re.compile("https?://[^ ]+$")


@functools.lru_cache
def is_url(text_string: str) -> bool:
    """Returns true if the text string is a url.

    This function is used in the templating to decide whether we should turn
    something into a hyperlink.  It's fairly conservative - the url needs to be
    fully qualified and start with http:// or https://

    """
    # as an optimisation, make sure it vaguely looks like a fully qualified url
    # before even trying to parse it
    if _URL_REGEX.match(text_string):
        try:
            urlparse(text_string)
            return True
        except ValueError:
            pass

    return False


def safe_redirect(to_raw: str) -> Response:
    """Redirect to a url, but only if it matches our server name.

    Intended for untrusted user-supplied input (ie: whence url params)."""
    to = urlparse(to_raw)
    base_url = urlparse(request.base_url)
    # relative link
    if to.scheme == "" and to.netloc == "":
        return unsafe_redirect(to_raw)
    elif to.scheme == base_url.scheme and to.netloc == base_url.netloc:
        return unsafe_redirect(to_raw)
    else:
        raise exc.InvalidRequest(f"won't redirect outside of {request.base_url}")


def register_and_sign_in_new_user(sesh: Session) -> User:
    """Registers a new user and signs them in if the registration succeeds."""
    form = request.form
    new_user = svc.create_user(
        sesh,
        current_app.config["CRYPT_CONTEXT"],
        form["username"],
        form["password"],
        form.get("email"),
        form.get("mailing-list", default=False, type=bool),
    )
    sign_in_user(new_user)
    flash("Account registered")
    return new_user


def sign_in_user(user: User, session: Optional[Any] = None) -> None:
    """Sets the current user and sets a cookie to keep them logged in across
    requests.

    """
    set_current_user(user)

    if session is None:
        session = flask_session
    session["user_uuid"] = user.user_uuid
    # Make it last for 31 days
    session.permanent = True


def ensure_table_access(
    sesh: Session, table: Table, mode: Union[Literal["read"], Literal["write"]]
) -> None:
    """Ensures that the current user can access the particular table in the
    given mode.

    """
    is_public = svc.is_public(sesh, table.username, table.table_name)
    if mode == "read":
        if not is_public and not am_user(table.username):
            raise exc.TableDoesNotExistException(table.username, table.table_name)
    else:
        if not am_user(table.username):
            if is_public and am_a_user():
                raise exc.NotAllowedException()
            elif is_public:
                raise exc.NotAuthenticatedException()
            else:
                raise exc.TableDoesNotExistException(table.username, table.table_name)
    return None


def ensure_not_read_only(table: Table) -> None:
    """Raises ReadOnlyException if a table is read-only.

    This is should be called before modifying userdata (but not when only
    modifying metadata).

    """
    source = table.external_source
    if source is not None:
        if source.is_read_only():
            raise exc.ReadOnlyException()


def am_user(username: str) -> bool:
    """Return true if the current user has the given username."""
    current_user = get_current_user()
    if current_user is None or current_user.username != username:
        return False
    else:
        return True


def am_a_user() -> bool:
    return get_current_user() is not None


def am_user_or_400(username: str) -> bool:
    if not am_user(username):
        raise exc.NotAuthenticatedException()
    return True


def am_a_user_or_400():
    if not am_a_user():
        raise exc.NotAuthenticatedException()
