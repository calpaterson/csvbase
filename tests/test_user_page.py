import base64
import re

from lxml import etree
import pytest

from csvbase import svc, models
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
        assert page.xpath(
            f"//h5[@class='card-title']/a[text()='{ten_rows_display_name}']"
        )
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
        assert page.xpath(
            f"//h5[@class='card-title']/a[text()='{ten_rows_display_name}']"
        )
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


def test_verify_email_address(sesh, client, test_user, mock_smtpd):
    test_user.email = "example@example.com"
    svc.update_user(sesh, test_user)
    sesh.commit()
    with current_user(test_user):
        verify_email_post = client.post("/verify-email")
        assert verify_email_post.status_code == 302
        verify_email_get = client.get(verify_email_post.headers["Location"])
        assert verify_email_get.status_code == 200

    mock_smtpd.join()
    user_email_obj = sesh.get(models.UserEmail, test_user.user_uuid)
    urlsafe_code: str = base64.urlsafe_b64encode(
        user_email_obj.verification_code
    ).decode("utf-8")
    expected_message_id = f"<verify-email-{urlsafe_code}@localhost>"
    mock_smtpd.join()

    email = mock_smtpd.received[expected_message_id]
    match_obj = re.search(re.compile(r"https?://[^\s]+"), email.get_content())
    verify_url = match_obj.group()  # type: ignore
    with current_user(test_user):
        resp = client.get(verify_url)
    assert resp.status_code == 200
