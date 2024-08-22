"""Support for Cloudflare Turnstile, a captcha system."""

from logging import getLogger

import werkzeug

from csvbase import exc
from csvbase.config import get_config
from csvbase.http import http_sesh

VERIFY_URL = "https://challenges.cloudflare.com/turnstile/v0/siteverify"

# Wait about 6s to connect, 24 secs for first byte
TIMEOUT = (6.1, 24)

logger = getLogger(__name__)


def get_turnstile_token_from_form(form: werkzeug.datastructures.MultiDict) -> str:
    token = form.get("cf-turnstile-response", None)
    if token is None:
        raise exc.InvalidRequest()
    else:
        return token


def validate_turnstile_token(turnstile_token: str) -> None:
    """Check that a turnstile token is valid."""
    # FIXME: It is possibly worthwhile adding a way to make this "fail open".
    # Currently it is "fail closed" (ie if we can't connect to
    # challenges.cloudflare.com then we fail.

    secret_key = get_config().turnstile_secret_key
    if secret_key is None:
        logger.warning("turnstile key not set, not checking token")
        return

    resp = http_sesh.post(
        VERIFY_URL,
        data={
            "secret": secret_key,
            "response": turnstile_token,
        },
        timeout=TIMEOUT,
    )
    response_doc = resp.json()
    logger.info("got response doc %s", response_doc)
    if not response_doc.get("success", False):
        error_codes = response_doc.get("error-codes")
        logger.error("captcha check failed for reasons: '%s'", error_codes)
        if (
            "invalid-input-response" in error_codes
            or "timeout-or-duplicate" in error_codes
        ):
            raise exc.CaptchaFailureException()
        else:
            raise RuntimeError(f"Cloudflare turnstile error: {error_codes}")
