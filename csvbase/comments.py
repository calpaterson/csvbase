import re
import secrets
from dataclasses import dataclass
from typing import Sequence
from uuid import UUID

from sqlalchemy.orm import Session

from . import svc
from .value_objs import Comment, Thread


@dataclass
class CommentPage:
    thread: Thread
    comments: Sequence[Comment]


SLUG_PREFIX_REGEX = re.compile(r"[^a-z09]")


def _create_thread_slug(sesh: Session, title: str) -> str:
    attempt = 1
    max_attempts = 32
    prefix = re.sub(SLUG_PREFIX_REGEX, "-", title[:15].lower())
    while attempt <= max_attempts:
        # a little bit of randomness is appended to make threads harder to enumerate
        slug = "".join([prefix, secrets.token_hex(attempt + 2)])
        # FIXME: check not exists
        return slug
    else:
        raise RuntimeError("unable to create slug!")


def get_comment_page(sesh: Session, thread_slug: str) -> CommentPage:
    from datetime import datetime, timezone

    thread = Thread(
        slug="test",
        title="Test thread",
        created=datetime.now(timezone.utc),
        updated=datetime.now(timezone.utc),
    )
    comments = [
        Comment(
            comment_id=n,
            user=svc.user_by_name(sesh, "calpaterson"),
            created=datetime.now(timezone.utc),
            updated=datetime.now(timezone.utc),
            markdown=f"hello, this is comment {n}",
        )
        for n in range(3)
    ]
    return CommentPage(thread=thread, comments=comments)


def create_thread(sesh: Session) -> Thread:  # type: ignore
    ...


def create_comment(sesh: Session, thread_slug: int, user_uuid: UUID, title: str, markdown: str) -> Comment:  # type: ignore
    ...
