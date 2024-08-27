import pytest
from csvbase.value_objs import Thread
from csvbase import comments_svc

from . import utils


def test_create_slug(sesh):
    title = "What does null mean in this context?"
    slug = comments_svc._create_thread_slug(sesh, title)
    assert slug.startswith("what-does-null")


@pytest.fixture
def test_thread(module_sesh, test_user) -> Thread:
    thread = comments_svc.create_thread_with_opening_comment(
        module_sesh, test_user, "Test thread", "Hello"
    )
    module_sesh.commit()
    return thread


def test_comment__create(sesh, client, test_thread, test_user):
    with utils.current_user(test_user):
        resp = client.post(
            f"/threads/{test_thread.slug}",
            data={
                "comment-markdown": "hello",
            },
        )
    assert resp.status_code == 302

    thread_page = comments_svc.get_comment_page(sesh, test_thread.slug)
    assert len(thread_page.comments) == 2


@pytest.mark.parametrize("comment_id, expected_page_number", [
    (1, 1),
    (9, 1),
    (10, 1),
    (11, 2),
    (19, 2),
    (20, 2),
    (21, 3),
])
def test_comment_id_to_page_number(comment_id, expected_page_number):
    actual_page_number = comments_svc.comment_id_to_page_number(comment_id)
    assert expected_page_number == actual_page_number


@pytest.mark.parametrize("page_number, expected_first_comment_id", [
    (1, 1),
    (2, 11),
    (3, 21),
])
def test_page_number_to_first_comment_id(page_number, expected_first_comment_id):
    actual_first_comment_id = comments_svc.page_number_to_first_comment_id(page_number)
    assert actual_first_comment_id == expected_first_comment_id
