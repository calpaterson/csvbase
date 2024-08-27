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
