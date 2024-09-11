import json
from datetime import timedelta, datetime, timezone

from feedgen.feed import FeedGenerator
from flask import Blueprint, render_template, Response, url_for, make_response, request
from sqlalchemy.orm import Session

from .value_objs import Post
from . import svc as blog_svc
from ... import exc, comments_svc
from csvbase.sesh import get_sesh
from csvbase.markdown import render_markdown

bp = Blueprint("blog", __name__)

# Just enough to keep the req/s down
CACHE_TTL = int(timedelta(minutes=3).total_seconds())


@bp.get("/blog")
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

    if post_obj.thread_slug is not None:
        comment_page = comments_svc.get_comment_page(
            sesh, post_obj.thread_slug, start=1
        )
        max_comment_page_number = comments_svc.comment_id_to_page_number(
            comments_svc.get_max_comment_id(sesh, post_obj.thread_slug) or 1
        )
    else:
        comment_page = None
        max_comment_page_number = None

    response = make_response(
        render_template(
            "post.html",
            post=post_obj,
            rendered=md,
            page_title=post_obj.title,
            ld_json=ld_json,
            canonical_url=post_url,
            comment_page=comment_page,
            max_comment_page_number=max_comment_page_number,
        )
    )
    cc = response.cache_control
    if post_obj.draft:
        cc.no_store = True
        cc.private = True
    else:
        cc.max_age = CACHE_TTL
    return response


@bp.get("/blog/posts.rss")
def rss() -> Response:
    sesh = get_sesh()
    feed = make_feed(sesh, url_for("blog.rss", _external=True))
    response = Response(feed, mimetype="application/rss+xml")
    cc = response.cache_control
    # RSS feed updates need to be picked up in reasonable period of time
    cc.public = True
    cc.max_age = int(timedelta(days=1).total_seconds())
    return response


def make_feed(sesh: Session, feed_url: str) -> str:
    fg = FeedGenerator()
    fg.id(feed_url)
    fg.title("csvbase blog")
    fg.language("en")
    fg.link(href=feed_url, rel="self")
    fg.description("The csvbase blog")

    for post in blog_svc.get_posts(sesh):
        if post.draft:
            continue
        fe = fg.add_entry()
        fe.id(str(post.uuid))
        fe.title(post.title)
        if post.posted:
            posted = post.posted
            fe.pubDate(
                datetime(posted.year, posted.month, posted.day, tzinfo=timezone.utc)
            )
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
