import pytest

from .utils import random_string


@pytest.mark.parametrize(
    "verb, url, expected_status_code",
    [
        pytest.param("get", f"/{random_string()}", 404, id="non existent user"),
        pytest.param("get", "/user/table/madeup", 404, id="non existent page"),
        pytest.param("delete", "/", 405, id="not a supported verb"),
    ],
)
@pytest.mark.parametrize(
    "accept",
    [
        pytest.param("text/html", id="browser"),
        pytest.param("*/*", id="curl"),
    ],
)
def test_404_error_pages(client, verb, url, expected_status_code, accept):
    if expected_status_code == 404:
        expected_message = "does not exist"
    else:
        expected_message = "that verb is not allowed"

    resp = client.open(url, method=verb, headers={"Accept": accept})

    assert resp.status_code == expected_status_code
    if accept == "text/html":
        assert (
            expected_message in resp.text
        )  # checking that our error page was generated
    else:
        assert expected_message in resp.json["error"]
