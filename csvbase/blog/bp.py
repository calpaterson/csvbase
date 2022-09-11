from typing import Optional
from uuid import UUID
from datetime import date
from dataclasses import dataclass

from flask import Blueprint, render_template
import marko

from .value_objs import Post
from . import svc as blog_svc

bp = Blueprint("blog", __name__)


@bp.route("/blog")
def blog_index():
    return render_template("blog.html", posts=blog_svc.get_posts())


@bp.route("/blog/<post_slug>")
def post(post_slug):
    return render_template("post.html", post=blog_svc.get_post(post_slug))
