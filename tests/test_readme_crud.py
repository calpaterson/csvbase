from csvbase.value_objs import ContentType

from .utils import current_user


def test_read__happy(client, test_user, ten_rows):
    response = client.get(f"/{test_user.username}/{ten_rows.table_name}/readme")
    assert response.status_code == 200
    assert response.content_type == ContentType.MARKDOWN.value


def test_write__happy(client, test_user, ten_rows):
    url = f"/{test_user.username}/{ten_rows.table_name}/readme"
    new_readme = "hello, *world*"

    with current_user(test_user):
        put_response = client.put(url, data=new_readme)
        assert put_response.status_code == 200

    get_response = client.get(url)
    assert get_response.status_code == 200
    assert get_response.text == new_readme


def test_write__too_big(client, test_user, ten_rows):
    url = f"/{test_user.username}/{ten_rows.table_name}/readme"
    new_readme = "f" * 10_001

    with current_user(test_user):
        put_response = client.put(url, data=new_readme)
    assert put_response.status_code == 400
