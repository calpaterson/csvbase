from typing import Sequence
from uuid import UUID

from .value_objs import Post

frist_post = Post(
    "frist",
    "Hello, World",
    UUID("edf795a0-93a9-4b5e-962a-c4194e3fddbb"),
    description="The first post",
    draft=False,
    markdown="Hi, so about *csvbase*...",
    cover_image_url="http://example.com/some.jpg",
    cover_image_alt="some jpg"
)


def get_posts() -> Sequence[Post]:
    return [frist_post]


def get_post(post_slug: str) -> Post:
    return frist_post
