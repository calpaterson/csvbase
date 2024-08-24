from flask import make_response, render_template
from flask.views import MethodView
from werkzeug.wrappers.response import Response

from csvbase import comments
from .bp import bp
from ...sesh import get_sesh


class ThreadView(MethodView):
    def get(self, thread_slug: str) -> Response:
        """Get a thread by slug"""
        sesh = get_sesh()
        comment_page = comments.get_comment_page(sesh, thread_slug)
        return make_response(render_template("thread.html", comment_page=comment_page))

    def post(self, thread_slug: str) -> Response:  # type: ignore
        """Add a comment to a thread"""


bp.add_url_rule("/threads/<thread_slug>", view_func=ThreadView.as_view("thread_view"))


class CommentView(MethodView):
    def get(self, thread_slug: str, comment_id: int) -> Response:  # type: ignore
        """Get an individual comment"""

    def post(self, thread_slug: str, comment_id: int) -> Response:  # type: ignore
        """Edit a comment"""
