from typing import Optional, Callable, Mapping, Tuple
from logging import getLogger
from urllib.parse import urlsplit

import werkzeug
import werkzeug.exceptions
from flask import request, g, current_app, Request

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
