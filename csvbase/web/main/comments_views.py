from flask import make_response, render_template, request, redirect, url_for, Blueprint
from flask.views import MethodView
from werkzeug.wrappers.response import Response

from csvbase import comments_svc
from ..func import get_current_user_or_401
from ...sesh import get_sesh


class ThreadView(MethodView):
    def get(self, thread_slug: str) -> Response:
        """Get a thread by slug"""
        sesh = get_sesh()
        comment_page = comments_svc.get_comment_page(sesh, thread_slug)
        return make_response(
            render_template(
                "thread.html",
                comment_page=comment_page,
                page_title=comment_page.thread.title,
            )
        )

    def post(self, thread_slug: str) -> Response:
        """Add a comment to a thread"""
        poster = get_current_user_or_401()
        sesh = get_sesh()
        thread = comments_svc.get_thread_by_slug(sesh, thread_slug)
        comments_svc.create_comment(
            sesh, poster, thread, request.form["comment-markdown"]
        )
        sesh.commit()
        return redirect(url_for("csvbase.thread_view", thread_slug=thread_slug))


class CommentView(MethodView):
    def get(self, thread_slug: str, comment_id: int) -> Response:  # type: ignore
        """Get an individual comment"""

    def post(self, thread_slug: str, comment_id: int) -> Response:  # type: ignore
        """Edit a comment"""

    def delete(self, thread_slug: str, comment_id: int) -> Response:  # type: ignore
        """Delete a comment.

        Browsers cannot actually call this so it is called from a tramponline below.

        """
        ...


def delete_comment_for_browsers(thread_slug: str, comment_id: int) -> Response:
    return CommentView().delete(thread_slug, comment_id)


def comment_edit_form(thread_slug: str, comment_id: int) -> Response: ...  # type: ignore


def init_comments_views(bp: Blueprint) -> None:
    bp.add_url_rule(
        "/threads/<thread_slug>", view_func=ThreadView.as_view("thread_view")
    )
    bp.add_url_rule(
        "/threads/<thread_slug>/<int:comment_id>/edit-form",
        view_func=comment_edit_form,
        methods=["GET"],
    )
    bp.add_url_rule(
        "/threads/<thread_slug>/<int:comment_id>/delete-for-browsers",
        view_func=delete_comment_for_browsers,
        methods=["POST"],
    )
