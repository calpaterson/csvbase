import re
import secrets
from dataclasses import dataclass
from typing import Sequence, Optional

from sqlalchemy.orm import Session
from sqlalchemy import func
from sqlalchemy.sql import exists

from . import svc, models
from .value_objs import Comment, Thread, User


@dataclass
class CommentPage:
    thread: Thread
    comments: Sequence[Comment]
    has_more: int
    has_less: int

    def comment_by_id(self, comment_id: int) -> Optional[Comment]:
        for comment in self.comments:
            if comment.comment_id == comment_id:
                return comment
        return None

    def page_number(self) -> int:
        if len(self.comments) == 0:
            return 1
        else:
            return comment_id_to_page_number(self.comments[0].comment_id)


def comment_id_to_page_number(comment_id: int) -> int:
    return ((comment_id - 1) // 10) + 1

def page_number_to_first_comment_id(page_number: int) -> int:
    return (page_number * 10) - 9


SLUG_PREFIX_REGEX = re.compile(r"[^a-z09]")


def _create_thread_slug(sesh: Session, title: str) -> str:
    """Create a thread slug.

    To avoid enumeration of threads, the thread slug is a bit of the title
    (downcased and clamped to [a-z0-9]+) plus a bit of randomness.

    To avoid clashes the amount of randomness is increased if we find a slug
    that exists.
    """
    attempt = 1
    max_attempts = 14
    prefix = re.sub(SLUG_PREFIX_REGEX, "-", title[:15].lower())
    while attempt <= max_attempts:
        # a little bit of randomness is appended to make threads harder to enumerate
        slug = "-".join([prefix, secrets.token_hex(attempt + 2)])
        slug_exists: bool = sesh.query(
            exists().where(models.Thread.thread_slug == slug)
        ).scalar()
        if not slug_exists:
            return slug
    else:
        # 16 bytes of randomness is a lot, this won't happen unless something
        # is very wrong
        raise RuntimeError("unable to create slug!")


def _comment_obj_to_comment(
    sesh, thread: Thread, comment_obj: models.Comment
) -> Comment:
    # this isn't really workable, want to avoid double lookups of users
    return Comment(
        thread=thread,
        comment_id=comment_obj.comment_id,
        user=svc.user_by_user_uuid(sesh, comment_obj.user_uuid),
        created=comment_obj.created,
        updated=comment_obj.updated,
        markdown=comment_obj.comment_markdown,
        referenced_by=[],
    )


def get_comment_page(
    sesh: Session, thread_slug: str, start: int = 1, count: int = 10
) -> CommentPage:
    """Return a page of comments, keyset style."""
    thread = get_thread_by_slug(sesh, thread_slug)
    comment_objs = (
        sesh.query(models.Comment)
        .filter(
            models.Comment.thread_id == thread.internal_thread_id,
            models.Comment.comment_id >= start,
        )
        .order_by(models.Comment.comment_id)
        .limit(count + 1)
        .all()
    )
    if len(comment_objs) > count:
        has_more = True
        del comment_objs[-1]
    else:
        has_more = False
    comments = [
        _comment_obj_to_comment(sesh, thread, comment_obj)
        for comment_obj in comment_objs
    ]

    return CommentPage(thread=thread, comments=comments, has_more=has_more, has_less=start>1)


def get_comment(sesh: Session, thread: Thread, comment_id: int) -> Comment:
    # FIXME: error handling
    comment = (
        sesh.query(models.Comment)
        .filter(
            models.Comment.thread_id == thread.internal_thread_id,
            models.Comment.comment_id == comment_id,
        )
        .one()
    )
    return _comment_obj_to_comment(sesh, thread, comment)


def get_max_comment_id(sesh: Session, thread_slug: str) -> Optional[int]:
    return (
        sesh.query(func.max(models.Comment.comment_id))
        .join(models.Thread, models.Thread.thread_id == models.Comment.thread_id)
        .filter(models.Thread.thread_slug==thread_slug)
        .scalar()
    )




def create_thread_with_opening_comment(
    sesh: Session, creator: User, title: str, first_comment_markdown: str
) -> Thread:
    """Create a new thread, with a first comment."""
    thread = create_thread(sesh, creator, title)
    create_comment(sesh, creator, thread, first_comment_markdown)
    return thread


def create_thread(sesh: Session, creator: User, title: str) -> Thread:
    """Create a new thread."""
    thread_obj = models.Thread(
        user_uuid=creator.user_uuid,
        thread_title=title,
        thread_slug=_create_thread_slug(sesh, title),
    )
    sesh.add(thread_obj)
    sesh.flush()  # get the internal thread id
    return Thread(
        slug=thread_obj.thread_slug,
        title=title,
        created=thread_obj.created,
        updated=thread_obj.updated,
        creator=creator,
        internal_thread_id=thread_obj.thread_id,
    )


def create_comment(
    sesh: Session, owner: User, thread: Thread, markdown: str
) -> Comment:
    comment_obj = models.Comment(
        comment_id=_next_comment_id(sesh, thread.internal_thread_id),
        user_uuid=owner.user_uuid,
        thread_id=thread.internal_thread_id,
        comment_markdown=markdown,
    )
    sesh.add(comment_obj)
    sesh.flush()
    comment = Comment(
        thread=thread,
        comment_id=comment_obj.comment_id,
        user=owner,
        created=comment_obj.created,
        updated=comment_obj.updated,
        markdown=markdown,
        referenced_by=[],
    )
    return comment


def get_thread_by_slug(sesh: Session, thread_slug: str) -> Thread:
    thread_obj = (
        sesh.query(models.Thread).filter(models.Thread.thread_slug == thread_slug).one()
    )

    # this seems sub-optimal but they will very likely be sitting in cache:
    user = svc.user_by_user_uuid(sesh, thread_obj.user_uuid)

    thread = Thread(
        slug=thread_obj.thread_slug,
        title=thread_obj.thread_title,
        created=thread_obj.created,
        updated=thread_obj.updated,
        creator=user,
        internal_thread_id=thread_obj.thread_id,
    )
    return thread


def _next_comment_id(sesh: Session, internal_thread_id: int) -> int:
    """Comment ids are auto-incrementing, but per-thread.

    If writing later raises an IntegrityError, someone else posted before you
    did.

    """
    return (
        sesh.query(func.coalesce(func.max(models.Comment.comment_id), 0) + 1)
        .filter(models.Comment.thread_id == internal_thread_id)
        .scalar()
    )
