import pytest

from csvbase.value_objs import Thread
from csvbase import comments_svc, models

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


@pytest.mark.parametrize(
    "comment_id, expected_page_number",
    [
        (1, 1),
        (9, 1),
        (10, 1),
        (11, 2),
        (19, 2),
        (20, 2),
        (21, 3),
    ],
)
def test_comment_id_to_page_number(comment_id, expected_page_number):
    actual_page_number = comments_svc.comment_id_to_page_number(comment_id)
    assert expected_page_number == actual_page_number


@pytest.mark.parametrize(
    "page_number, expected_first_comment_id",
    [
        (1, 1),
        (2, 11),
        (3, 21),
    ],
)
def test_page_number_to_first_comment_id(page_number, expected_first_comment_id):
    actual_first_comment_id = comments_svc.page_number_to_first_comment_id(page_number)
    assert actual_first_comment_id == expected_first_comment_id


# def extract_comments(resp) -> Dict[int, str]:
#     comment_div_sel = CSSSelector(".comment")
#     markdown_sel = CSSSelector(".card-body")
#     html_parser = etree.HTMLParser()
#     root = etree.fromstring(resp.data, html_parser)
#     for comment_div in comment_div_sel(root):
#         breakpoint()
#         id = root.id


def test_comment__create(sesh, client, test_thread, test_user, requests_mocker):
    utils.mock_turnstile(requests_mocker)
    with utils.current_user(test_user):
        resp = client.post(
            f"/threads/{test_thread.slug}",
            data={
                "comment-markdown": "hello",
                "cf-turnstile-response": utils.random_string(),
            },
        )
    assert resp.status_code == 302

    thread_page = comments_svc.get_comment_page(sesh, test_thread.slug)
    assert len(thread_page.comments) == 2


# def test_comment__edit(sesh, client, test_thread, test_user):
#     comment_text = utils.random_string()
#     with utils.current_user(test_user):
#         edit_form_resp = client.get(f"/threads/{test_thread.slug}/1/edit-form")
#         assert edit_form_resp.status_code == 200

#         edit_resp = client.post(
#             f"/threads/{test_thread.slug}", data={"comment-markdown": comment_text}
#         )
#     assert edit_resp.status_code == 302
#     follow_resp = client.get(edit_resp["Location"])
#     comments = extract_comments(follow_resp)
#     assert comment_text in comments[1]


def test_set_references(sesh, test_thread, test_user):
    def get_current_references(comment_id):
        return {
            x[0]
            for x in sesh.query(models.CommentReference.referenced_comment_id).filter(
                models.CommentReference.thread_id == test_thread.internal_thread_id,
                models.CommentReference.comment_id == comment_id,
            )
        }

    for n in range(3):
        comments_svc.create_comment(sesh, test_user, test_thread, f"Test post #{n}")

    comments_svc.set_references(sesh, test_thread, 4, ["#1", "#2", "#3"])
    assert get_current_references(4) == {1, 2, 3}

    comments_svc.set_references(sesh, test_thread, 4, ["#2", "#3", "#4"])
    assert get_current_references(4) == {2, 3, 4}
