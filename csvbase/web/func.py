from datetime import datetime, timezone
from typing import Optional, Callable, Mapping, Tuple
from logging import getLogger
from urllib.parse import urlsplit

import werkzeug
import werkzeug.exceptions
from flask import request, g, current_app, Request
from flask_babel import get_locale, dates

from .. import exc
from .. import sentry
from ..value_objs import User

logger = getLogger(__name__)


def is_browser() -> bool:
    # bit of content negotiation magic
    accepts = werkzeug.http.parse_accept_header(request.headers.get("Accept"))
    best = accepts.best_match(["text/html", "text/csv"], default="text/csv")
    return best == "text/html"


def set_current_user(user: User) -> None:
    g.current_user = user

    # This is duplication but very convenient for jinja templates
    g.current_username = user.username

    sentry.set_user(user)


def get_current_user() -> Optional[User]:
    """Return the current user.  This function exists primarily for type
    checking reasons - to avoid accidental assumptions that g.current_user is
    present.

    """
    if hasattr(g, "current_user"):
        return g.current_user
    else:
        return None


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
