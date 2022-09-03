from typing import Optional
from uuid import UUID
from datetime import date
from dataclasses import dataclass

from flask import Blueprint, render_template
import marko

bp = Blueprint("blog", __name__)

@dataclass
class Post:
    slug: str
    title: str
    uuid: UUID
    description: str
    draft: bool
    markdown: str
    posted: Optional[date] = None
    cover_image: Optional[str] = None
    cover_image_alt: Optional[str] = None

    def render_markdown(self) -> str:
        return marko.convert(self.markdown)

    def render_posted(self) -> str:
        if self.posted is not None:
            return self.posted.isoformat()
        else:
            return "(not posted yet)"


frist_post = Post(
    "frist",
    "Hello, World",
    UUID("edf795a0-93a9-4b5e-962a-c4194e3fddbb"),
    description="The first post",
    draft=False,
    markdown="Hi, so about *csvbase*...",
)


@bp.route("/blog")
def blog_index():
    return render_template("blog.html", posts=[frist_post])


@bp.route("/blog/<post_slug>")
def post(post_slug):
    return render_template("post.html", post=frist_post)
