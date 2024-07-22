from lxml import etree
import pytest

from csvbase.value_objs import ContentType

from .utils import make_user, current_user

@pytest.fixture(scope="module", params=[ContentType.HTML, ContentType.JSON])
def accept(request):
    yield request.param


def test_user_view__self(client, test_user, ten_rows, private_table, accept):
    with current_user(test_user):
        resp = client.get(f"/{test_user.username}", headers={"Accept": accept.value})

    ten_rows_display_name = test_user.username + "/" + ten_rows.table_name
    private_table_display_name = test_user.username + "/" + private_table

    page = etree.HTML(resp.text)

    assert resp.status_code == 200
    if accept is ContentType.HTML:
        assert page.xpath(f"//h5[@class='card-title']/a[text()='{ten_rows_display_name}']")
        assert page.xpath(
            f"//h5[@class='card-title']/a[text()='{private_table_display_name}']"
        )
    else:
        assert resp.json is not None


def test_user_view__while_anon(client, test_user, ten_rows, private_table, accept):
    resp = client.get(f"/{test_user.username}", headers={"Accept": accept.value})

    ten_rows_display_name = test_user.username + "/" + ten_rows.table_name
    private_table_display_name = test_user.username + "/" + private_table

    page = etree.HTML(resp.text)

    assert resp.status_code == 200
    if accept is ContentType.HTML:
        assert page.xpath(f"//h5[@class='card-title']/a[text()='{ten_rows_display_name}']")
        assert not page.xpath(
            f"//h5[@class='card-title']/a[text()='{private_table_display_name}']"
        )
    else:
        assert resp.json is not None


def test_user_view__other(app, sesh, client, test_user, ten_rows, accept):
    with current_user(make_user(sesh, app.config["CRYPT_CONTEXT"])):
        resp = client.get(f"/{test_user.username}", headers={"Accept": accept.value})
    assert resp.status_code == 200
    if accept is ContentType.JSON:
        assert resp.json is not None
