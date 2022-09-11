from typing import Optional
from uuid import UUID
from datetime import date
from dataclasses import dataclass

from feedgen.feed import FeedGenerator
from flask import Blueprint, render_template, Response, url_for
import marko

from .value_objs import Post
from . import svc as blog_svc

bp = Blueprint("blog", __name__)


@bp.route("/blog")
def blog_index() -> str:
    return render_template("blog.html", posts=blog_svc.get_posts())


@bp.route("/blog/<post_slug>")
def post(post_slug) -> str:
    return render_template("post.html", post=blog_svc.get_post(post_slug))


@bp.route("/blog/posts.rss")
def rss() -> Response:
    feed = make_feed(url_for("blog.rss", _external=True))
    response = Response(feed, mimetype="application/rss+xml")
    return response


def make_feed(feed_url: str) -> str:
    fg = FeedGenerator()
    fg.id(feed_url)
    fg.title("csvbase blog")
    fg.language("en")
    fg.link(href=feed_url, rel="self")
    fg.description("nothing")

    for post in blog_svc.get_posts():
        fe = fg.add_entry()
        fe.id(str(post.uuid))
        fe.title(post.title)
        fe.description(post.description)
        fe.link(href=url_for("blog.post", post_slug=post.slug, _external=True))

    return fg.rss_str(pretty=True)
