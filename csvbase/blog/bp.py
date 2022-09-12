from typing import Optional
from uuid import UUID
from datetime import date
from dataclasses import dataclass

from feedgen.feed import FeedGenerator
from flask import Blueprint, render_template, Response, url_for
import marko
from sqlalchemy.orm import Session

from .value_objs import Post
from . import svc as blog_svc
from csvbase.sesh import get_sesh

bp = Blueprint("blog", __name__)


@bp.route("/blog")
def blog_index() -> str:
    sesh = get_sesh()
    return render_template("blog.html", posts=blog_svc.get_posts(sesh))


@bp.route("/blog/<int:post_id>", methods=["GET"])
def post(post_id: int) -> str:
    sesh = get_sesh()
    post_obj = blog_svc.get_post(sesh, post_id)
    md = render_md(post_obj.markdown)
    return render_template("post.html", post=post_obj, rendered=md)


@bp.route("/blog/posts.rss")
def rss() -> Response:
    sesh = get_sesh()
    feed = make_feed(sesh, url_for("blog.rss", _external=True))
    response = Response(feed, mimetype="application/rss+xml")
    return response


def make_feed(sesh: Session, feed_url: str) -> str:
    fg = FeedGenerator()
    fg.id(feed_url)
    fg.title("csvbase blog")
    fg.language("en")
    fg.link(href=feed_url, rel="self")
    fg.description("nothing")

    for post in blog_svc.get_posts(sesh):
        fe = fg.add_entry()
        fe.id(str(post.uuid))
        fe.title(post.title)
        fe.description(post.description)
        fe.link(href=url_for("blog.post", post_id=post.id, _external=True))

    return fg.rss_str(pretty=True)


def render_md(markdown: str):
    return marko.convert(markdown)
