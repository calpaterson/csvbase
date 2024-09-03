from datetime import timedelta
from logging import getLogger
from urllib.parse import urljoin
import hashlib

import requests
from flask import make_response, Blueprint
from flask.wrappers import Response as FlaskResponse

from csvbase.http import http_sesh, BASIC_TIMEOUT
from csvbase.sesh import get_sesh
from csvbase.value_objs import User
from csvbase import svc

logger = getLogger(__name__)

bp = Blueprint("avatars", __name__)

BASE_URL = "https://gravatar.com/avatar/"


def get_gravatar(user: User) -> requests.Response:
    """Makes a request to the gravatar REST API."""
    if user.email is None:
        # They serve the default gravatar for the base url
        url = BASE_URL
    else:
        hashed_email = hashlib.sha256(user.email.lower().encode("utf-8")).hexdigest()
        url = urljoin(BASE_URL, hashed_email)
        logger.info("getting gravatar for '%s' (%s)", user.username, hashed_email)
    resp = http_sesh.get(url, params={"d": "mp"}, timeout=BASIC_TIMEOUT)
    resp.raise_for_status()
    return resp


@bp.get("/avatars/<username>")
def image(username: str) -> FlaskResponse:
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    gravatar_response = get_gravatar(user)
    response = make_response(gravatar_response.content)
    response.headers.set(
        "Content-Type",
        gravatar_response.headers.get("Content-Type", "application/octet-stream"),
    )

    # cache it
    cc = response.cache_control
    cc.max_age = int(timedelta(days=1).total_seconds())
    return response
