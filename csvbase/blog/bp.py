import json
from datetime import timedelta

from feedgen.feed import FeedGenerator
from flask import Blueprint, render_template, Response, url_for, make_response, request
from sqlalchemy.orm import Session

from .value_objs import Post
from . import svc as blog_svc
from .. import exc
from csvbase.sesh import get_sesh
from csvbase.markdown import render_markdown

bp = Blueprint("blog", __name__)

# a shortish time initially, increase this once confidence grows
CACHE_TTL = int(timedelta(hours=1).total_seconds())


@bp.route("/blog")
def blog_index() -> Response:
    sesh = get_sesh()
    posts = [post for post in blog_svc.get_posts(sesh) if not post.draft]
    response = make_response(
        render_template("blog.html", posts=posts, page_title="The csvbase blog")
    )
    cc = response.cache_control
    cc.max_age = CACHE_TTL
    return response


@bp.route("/blog/<int:post_id>", methods=["GET"])
def post(post_id: int) -> Response:
    sesh = get_sesh()
    not_found_message = "http error code 404: blog post not found"
    try:
        post_obj = blog_svc.get_post(sesh, post_id)
    except exc.RowDoesNotExistException:
        response = make_response(not_found_message)
        response.status_code = 404
        return response
    if post_obj.draft and request.args.get("uuid", "") != str(post_obj.uuid):
        response = make_response(not_found_message)
        response.status_code = 404
        return response
    md = render_markdown(post_obj.markdown)
    post_url = url_for("blog.post", post_id=post_id, _external=True)
    ld_json = make_ld_json(post_obj, post_url)
    response = make_response(
        render_template(
            "post.html",
            post=post_obj,
            rendered=md,
            page_title=post_obj.title,
            ld_json=ld_json,
            canonical_url=post_url,
        )
    )
    cc = response.cache_control
    if post_obj.draft:
        cc.no_store = True
    else:
        cc.max_age = CACHE_TTL
    return response


@bp.route("/blog/posts.rss")
def rss() -> Response:
    sesh = get_sesh()
    feed = make_feed(sesh, url_for("blog.rss", _external=True))
    response = Response(feed, mimetype="application/rss+xml")
    cc = response.cache_control
    # RSS feed updates need to be picked up in reasonable period of time
    cc.max_age = int(timedelta(days=1).total_seconds())
    return response


def make_feed(sesh: Session, feed_url: str) -> str:
    fg = FeedGenerator()
    fg.id(feed_url)
    fg.title("csvbase blog")
    fg.language("en")
    fg.link(href=feed_url, rel="self")
    fg.description("nothing")

    for post in blog_svc.get_posts(sesh):
        if post.draft:
            continue
        fe = fg.add_entry()
        fe.id(str(post.uuid))
        fe.title(post.title)
        fe.description(post.description)
        fe.link(href=url_for("blog.post", post_id=post.id, _external=True))

    return fg.rss_str(pretty=True)


def make_ld_json(post: Post, post_url: str) -> str:
    document = {
        "@context": "https://schema.org",
        "@type": "BlogPosting",
        "headline": post.title,
        "url": post_url,
        "description": post.description,
        "author": {
            "@type": "Person",
            "name": "Cal Paterson",
            "email": "cal@calpaterson.com",
            "url": "https://calpaterson.com/about.html",
        },
        "publisher": {
            "@type": "Organization",
            "name": "Cal Paterson Ltd",
            "email": "cal@calpaterson.com",
            "url": "https://calpaterson.com/about.html",
            "logo": {
                "@type": "ImageObject",
                "url": "https://calpaterson.com/assets/favicon.png",
            },
        },
        "mainEntityOfPage": post_url,
    }
    document["image"] = post.cover_image_url
    if post.posted is not None:
        document["datePublished"] = post.posted.isoformat()
        document["dateCreated"] = post.posted.isoformat()
        document["dateModified"] = post.posted.isoformat()
    return json.dumps(document, indent=4)
