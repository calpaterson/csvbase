from flask import make_response, render_template, request, redirect, url_for, Blueprint
from flask.views import MethodView
from werkzeug.wrappers.response import Response

from csvbase import comments_svc, markdown
from csvbase.value_objs import Comment
from ..func import get_current_user_or_401
from ...sesh import get_sesh


class ThreadView(MethodView):
    def get(self, thread_slug: str) -> Response:
        """Get a thread by slug"""
        sesh = get_sesh()
        page = request.args.get("page", default=1, type=int)
        max_comment_id = comments_svc.get_max_comment_id(sesh, thread_slug)
        start = comments_svc.page_number_to_first_comment_id(page)
        comment_page = comments_svc.get_comment_page(sesh, thread_slug, start=start)

        if max_comment_id is None:
            max_page = 1
        else:
            max_page = comments_svc.comment_id_to_page_number(max_comment_id)

        reply_to = request.args.get("replyto", default=None, type=int)
        if reply_to is not None:
            comment: Comment = comment_page.comment_by_id(
                reply_to
            ) or comments_svc.get_comment(sesh, comment_page.thread, reply_to)
            comment_markdown = markdown.quote_markdown(comment.markdown) + "\n\n"
        else:
            comment_markdown = ""

        comment_lines = len(comment_markdown.splitlines())

        return make_response(
            render_template(
                "thread.html",
                comment_page=comment_page,
                page_title=comment_page.thread.title,
                comment_markdown=comment_markdown,
                comment_box_lines=comment_lines + 2,
                current_page=page,
                max_page=max_page
            )
        )

    def post(self, thread_slug: str) -> Response:
        """Add a comment to a thread"""
        poster = get_current_user_or_401()
        sesh = get_sesh()
        thread = comments_svc.get_thread_by_slug(sesh, thread_slug)
        comment = comments_svc.create_comment(
            sesh, poster, thread, request.form["comment-markdown"]
        )
        sesh.commit()
        url_for_args = {"thread_slug": thread_slug}
        page_number = comments_svc.comment_id_to_page_number(comment.comment_id)
        if page_number != 1:
            url_for_args["page"] = str(page_number)
        return redirect(url_for("csvbase.thread_view", **url_for_args))  # type: ignore


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


def comment_edit_form(thread_slug: str, comment_id: int) -> Response:
    sesh = get_sesh()
    thread = comments_svc.get_thread_by_slug(sesh, thread_slug)
    comment = comments_svc.get_comment(sesh, thread, comment_id)
    return make_response(render_template(
        "comment-edit.html",
        page_title=f"Editing comment #{comment.comment_id}",
        comment=comment,
        comment_markdown=comment.markdown,
        page_number=comments_svc.comment_id_to_page_number(comment.comment_id),
    ))


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
