from unittest.mock import ANY

from flask import url_for

import pytest


@pytest.mark.parametrize(
    "method, endpoint, endpoint_args",
    [
        ("GET", "csvbase.table_view", {"username": "test", "table_name": "test"}),
        ("POST", "csvbase.create_row", {"username": "test", "table_name": "test"}),
        (
            "PUT",
            "csvbase.update_row",
            {"username": "test", "table_name": "test", "row_id": 1},
        ),
        (
            "GET",
            "csvbase.get_row",
            {"username": "test", "table_name": "test", "row_id": 1},
        ),
        (
            "DELETE",
            "csvbase.delete_row",
            {"username": "test", "table_name": "test", "row_id": 1},
        ),
        ("PUT", "csvbase.table_view", {"username": "test", "table_name": "test"}),
    ],
)
def test_cors__allowed_urls(client, method, endpoint, endpoint_args):
    url = url_for(endpoint, **endpoint_args)
    resp = client.options(url, headers={"Access-Control-Request-Method": method})
    assert resp.status_code == 200
    access_control_headers = {
        k: v for k, v in resp.headers.items() if k.startswith("Access-Control-")
    }

    assert access_control_headers == {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Max-Age": str(8 * 60 * 60),
        "Access-Control-Allow-Methods": ANY,
    }
    assert method in access_control_headers["Access-Control-Allow-Methods"]
