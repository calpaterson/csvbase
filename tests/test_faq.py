from datetime import timedelta
import importlib_resources

from .utils import random_string

import pytest

slugs = [
    entry_fp.stem
    for entry_fp in importlib_resources.files("csvbase.web.faq.entries").iterdir()
    if entry_fp.suffix == ".md"
]

expected_max_age = int(timedelta(days=1).total_seconds())


def test_faq_index(client):
    resp = client.get("/faq")
    assert resp.status_code == 200
    assert resp.cache_control.max_age == expected_max_age


@pytest.mark.parametrize("slug", slugs)
def test_faq_entries__all(client, slug):
    resp = client.get(f"/faq/{slug}")
    assert resp.status_code == 200
    assert resp.cache_control.max_age == expected_max_age


def test_faq_entries__missing(client):
    resp = client.get(f"/faq/{random_string()}")
    assert resp.status_code == 404
