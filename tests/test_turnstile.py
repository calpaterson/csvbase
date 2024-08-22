import requests
import pytest

from csvbase import exc
from csvbase.web.turnstile import validate_turnstile_token

from .utils import mock_turnstile, random_string, TURNSTILE_URL


def test_turnstile__happy(app, requests_mocker):
    mock_turnstile(requests_mocker)
    validate_turnstile_token(random_string())


def test_turnstile__non_json_response(requests_mocker):
    """Make sure that the turnstile API being down results in an exception"""
    requests_mocker.post(TURNSTILE_URL, text="501 service down")
    with pytest.raises(requests.exceptions.JSONDecodeError):
        validate_turnstile_token(random_string())


@pytest.mark.parametrize(
    "error_codes",
    [
        ["invalid-input-response"],
        ["timeout-or-duplicate"],
    ],
)
def test_turnstile__non_success_for_user_reason(error_codes, requests_mocker):
    requests_mocker.post(
        TURNSTILE_URL, json={"success": False, "error-codes": error_codes}
    )
    with pytest.raises(exc.CaptchaFailureException):
        validate_turnstile_token(random_string())


@pytest.mark.parametrize(
    "error_codes",
    [
        ["missing-input-secret"],
        ["invalid-input-secret"],
        ["missing-input-response"],
        ["bad-request"],
        ["internal-error"],
        ["anything-else"],
    ],
)
def test_turnstile__non_success_for_other_reason(app, error_codes, requests_mocker):
    requests_mocker.post(
        TURNSTILE_URL, json={"success": False, "error-codes": error_codes}
    )
    with pytest.raises(RuntimeError) as e:
        validate_turnstile_token(random_string())
    for error_code in error_codes:
        assert error_code in str(e.value)
