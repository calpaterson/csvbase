from datetime import datetime, timezone
from typing import TYPE_CHECKING, Mapping
from uuid import UUID

from sqlalchemy import (
    types as satypes,
    ForeignKey,
    UniqueConstraint,
    func,
    ForeignKeyConstraint,
)
from sqlalchemy.orm import mapped_column, DeclarativeBase, relationship
from sqlalchemy.dialects.postgresql import BYTEA, UUID as _PGUUID
from sqlalchemy.schema import CheckConstraint, Identity, MetaData

naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


metadata = MetaData(naming_convention=naming_convention)


class Base(DeclarativeBase):
    metadata = metadata


# https://github.com/dropbox/sqlalchemy-stubs/issues/94
if TYPE_CHECKING:
    PGUUID = satypes.TypeEngine[UUID]
else:
    PGUUID = _PGUUID(as_uuid=True)


METADATA_SCHEMA_TABLE_ARG: Mapping[str, str] = {"schema": "metadata"}


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        # FIXME: this regex is incorrect - A-z allows other characters, such as _
        # the correct regex should be ^[A-z][-A-Za-z0-9]+$ but first
        # implementing this ban at application level before correcting this
        # here.
        CheckConstraint("username ~ '^[A-z][-A-z0-9]+$'", "username_format"),
        CheckConstraint("char_length(username) <= 200", "username_length"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    user_uuid = mapped_column(PGUUID, primary_key=True)
    username = mapped_column(satypes.String, nullable=False, unique=True, index=True)
    password_hash = mapped_column(satypes.String, nullable=False)
    timezone = mapped_column(satypes.String, nullable=False)
    registered = mapped_column(satypes.DateTime(timezone=True), nullable=False)
    mailing_list = mapped_column(satypes.Boolean, nullable=False)

    email_obj = relationship("UserEmail", uselist=False, backref="user")

    bio_obj = relationship("UserBio", uselist=False, backref="user")

    api_key = relationship("APIKey", uselist=False, backref="user")

    table_objs = relationship("Table", uselist=True, backref="user")

    praise_objs = relationship("Praise", uselist=True, backref="user_objs")

    stripe_customers_obj = relationship(
        "StripeCustomer", uselist=False, backref="user_obj"
    )

    payment_references = relationship(
        "PaymentReference", uselist=True, backref="user_objs"
    )


class UserBio(Base):
    __tablename__ = "user_bios"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True
    )
    user_bio_markdown = mapped_column(satypes.String(length=10_000), nullable=False)


class UserEmail(Base):
    __tablename__ = "user_emails"
    __table_args__ = (
        CheckConstraint("email_address ~ '@'", "email_address_not_blank"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True
    )
    email_address = mapped_column(
        satypes.String(length=200), nullable=False, index=True
    )


class APIKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True
    )
    api_key = mapped_column(BYTEA(length=16), nullable=False, unique=True, index=True)


class TableBackend(Base):
    __tablename__ = "table_backends"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    backend_id = mapped_column(
        satypes.SmallInteger, primary_key=True, autoincrement=False
    )
    backend_name = mapped_column(satypes.String, nullable=False)


def _created_default(context) -> datetime:
    """Returns value of 'created' attr.

    Useful when models have a created and an updated and upon creation they
    should match.

    """
    return context.get_current_parameters()["created"]


class Table(Base):
    __tablename__ = "tables"
    __table_args__ = (
        CheckConstraint("table_name ~ '^[A-z][-A-z0-9]+$'", "table_name_format"),
        CheckConstraint("char_length(table_name) <= 200", "table_name_length"),
        CheckConstraint("char_length(caption) <= 200", "caption_length"),
        UniqueConstraint("user_uuid", "table_name"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    table_uuid = mapped_column(PGUUID, primary_key=True)
    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False
    )
    public = mapped_column(satypes.Boolean, nullable=False)

    # Setting the default to datetime.now, rather than func.now(), helps tests
    # as otherwise multiple tables created in a single SQLA tx get the same
    # time
    created = mapped_column(
        satypes.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    licence_id = mapped_column(
        satypes.SmallInteger,
        ForeignKey("metadata.data_licences.licence_id"),
        nullable=False,
    )
    table_name = mapped_column(satypes.String, nullable=False, index=True)
    caption = mapped_column(
        satypes.String,
        nullable=False,
    )

    last_changed = mapped_column(
        satypes.DateTime(timezone=True),
        default=_created_default,
        nullable=False,
        index=True,
    )
    backend_id = mapped_column(
        satypes.SmallInteger, ForeignKey(TableBackend.backend_id), nullable=False
    )

    readme_obj = relationship("TableReadme", uselist=False, backref="table")

    praise_objs = relationship("Praise", uselist=True, backref="table_objs")
    backend_obj = relationship("TableBackend", uselist=False, backref="table_objs")
    git_upstream_obj = relationship("GitUpstream", uselist=False, backref="table_objs")


class UniqueColumn(Base):
    __tablename__ = "unique_columns"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    table_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), primary_key=True
    )
    column_name = mapped_column(satypes.String, nullable=False, primary_key=True)


class GitUpstream(Base):
    # FIXME: table should be called "git_upstreams"
    __tablename__ = "github_follows"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    table_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), primary_key=True
    )
    last_sha = mapped_column(BYTEA, nullable=False)
    last_modified = mapped_column(satypes.DateTime(timezone=True), nullable=False)
    https_repo_url = mapped_column(satypes.String, nullable=False)
    branch = mapped_column(satypes.String, nullable=False)
    path = mapped_column(satypes.String, nullable=False)
    # FIXME: to come
    # last_checked = mapped_column(satypes.DateTime(timezone=True), nullable=False)


class TableReadme(Base):
    __tablename__ = "table_readmes"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    table_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), primary_key=True
    )
    readme_markdown = mapped_column(satypes.String(length=10_000), nullable=False)


class DataLicence(Base):
    __tablename__ = "data_licences"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    licence_id = mapped_column(
        satypes.SmallInteger, primary_key=True, autoincrement=False
    )
    # FIXME: name should be nullable = false
    licence_name = mapped_column(satypes.String)


class Praise(Base):
    __tablename__ = "praise"
    __table_args__ = (
        UniqueConstraint("user_uuid", "table_uuid"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    praise_id = mapped_column(satypes.BigInteger, Identity(), primary_key=True)
    table_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), nullable=False, index=True
    )
    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False, index=True
    )
    praised = mapped_column(
        satypes.TIMESTAMP(timezone=True),
        nullable=False,
        index=True,
        server_default=func.current_timestamp(),
    )


class ProhibitedUsername(Base):
    __tablename__ = "prohibited_usernames"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    username = mapped_column(satypes.String, nullable=False, primary_key=True)


class PaymentReference(Base):
    __tablename__ = "payment_references"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    payment_reference_uuid = mapped_column(PGUUID, primary_key=True)
    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), index=True, nullable=False
    )
    payment_reference = mapped_column(
        satypes.String, index=True, unique=True, nullable=False
    )
    created = mapped_column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )


class StripeCustomer(Base):
    __tablename__ = "stripe_customers"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = mapped_column(
        PGUUID,
        ForeignKey("metadata.users.user_uuid"),
        primary_key=True,
    )
    stripe_customer_id = mapped_column(
        satypes.String, nullable=False, index=True, unique=True
    )
    created = mapped_column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )


class StripeSubscription(Base):
    __tablename__ = "stripe_subscriptions"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    stripe_subscription_id = mapped_column(satypes.String, primary_key=True)
    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False, index=True
    )
    stripe_subscription_status_id = mapped_column(
        satypes.SmallInteger,
        ForeignKey(
            "metadata.stripe_subscription_statuses.stripe_subscription_status_id"
        ),
        index=True,
        nullable=False,
    )
    created = mapped_column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    updated = mapped_column(
        satypes.DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        index=True,
        onupdate=func.now(),
    )
    ttl = mapped_column(satypes.DateTime(timezone=True), nullable=False, index=True)


class StripeSubscriptionStatus(Base):
    __tablename__ = "stripe_subscription_statuses"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    stripe_subscription_status_id = mapped_column(
        satypes.SmallInteger, primary_key=True, unique=True, autoincrement=False
    )
    stripe_subscription_status = mapped_column(
        satypes.String, nullable=False, unique=True
    )


class Copy(Base):
    __tablename__ = "copies"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    copy_id = mapped_column(satypes.BigInteger, Identity(), unique=True, index=True)
    from_uuid = mapped_column(
        PGUUID,
        ForeignKey("metadata.tables.table_uuid"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    to_uuid = mapped_column(
        PGUUID,
        ForeignKey("metadata.tables.table_uuid"),
        nullable=False,
        index=True,
        primary_key=True,
    )

    created = mapped_column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )

    from_obj = relationship(
        "Table",
        uselist=False,
        foreign_keys=[from_uuid],
    )
    to_obj = relationship(
        "Table",
        uselist=False,
        foreign_keys=[to_uuid],
    )


class Thread(Base):
    __tablename__ = "threads"
    __table_args__ = (
        CheckConstraint("char_length(thread_title) <= 500", "threads_title_length"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    thread_id = mapped_column(satypes.BigInteger, Identity(), primary_key=True)
    created = mapped_column(
        satypes.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    updated = mapped_column(
        satypes.DateTime(timezone=True),
        default=_created_default,
        nullable=False,
        index=True,
    )
    deleted = mapped_column(
        satypes.DateTime(timezone=True),
        default=None,
        nullable=True,
    )
    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False
    )
    thread_title = mapped_column(satypes.String, nullable=False)
    thread_slug = mapped_column(satypes.String, nullable=False, unique=True)

    user_obj = relationship("User", uselist=False)


class Comment(Base):
    __tablename__ = "comments"
    __table_args__ = (
        CheckConstraint(
            "char_length(comment_markdown) <= 4000", "comments_markdown_length"
        ),
        METADATA_SCHEMA_TABLE_ARG,
    )

    comment_id = mapped_column(satypes.BigInteger, primary_key=True)
    user_uuid = mapped_column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False, index=True
    )
    thread_id = mapped_column(
        satypes.BigInteger,
        ForeignKey("metadata.threads.thread_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )

    created = mapped_column(
        satypes.DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
        nullable=False,
        index=True,
    )
    updated = mapped_column(
        satypes.DateTime(timezone=True),
        default=_created_default,
        nullable=False,
        index=True,
    )
    deleted = mapped_column(
        satypes.DateTime(timezone=True),
        default=None,
        nullable=True,
    )
    comment_markdown = mapped_column(satypes.String, nullable=False)

    user_obj = relationship("User", uselist=False)
    thread_obj = relationship("Thread", uselist=False)


class CommentReference(Base):
    __tablename__ = "comment_references"
    __table_args__ = (
        ForeignKeyConstraint(
            ["thread_id", "comment_id"],
            ["metadata.comments.thread_id", "metadata.comments.comment_id"],
        ),
        ForeignKeyConstraint(
            ["referenced_thread_id", "referenced_comment_id"],
            ["metadata.comments.thread_id", "metadata.comments.comment_id"],
        ),
        METADATA_SCHEMA_TABLE_ARG,
    )

    thread_id = mapped_column(
        satypes.BigInteger,
        # ForeignKey("metadata.threads.thread_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    comment_id = mapped_column(
        satypes.BigInteger,
        # ForeignKey("metadata.comments.comment_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    referenced_thread_id = mapped_column(
        satypes.BigInteger,
        # ForeignKey("metadata.threads.thread_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    referenced_comment_id = mapped_column(
        satypes.BigInteger,
        # ForeignKey("metadata.comments.comment_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )

    comment_obj = relationship("Comment", foreign_keys=[thread_id, comment_id])
    referenced_comment_obj = relationship(
        "Comment", foreign_keys=[referenced_thread_id, referenced_comment_id]
    )


class RowReference(Base):
    __tablename__ = "row_references"
    __table_args__ = (
        ForeignKeyConstraint(
            ["thread_id", "comment_id"],
            ["metadata.comments.thread_id", "metadata.comments.comment_id"],
        ),
        METADATA_SCHEMA_TABLE_ARG,
    )

    thread_id = mapped_column(
        satypes.BigInteger,
        # ForeignKey("metadata.threads.thread_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    comment_id = mapped_column(
        satypes.BigInteger,
        # ForeignKey("metadata.comments.comment_id"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    referenced_table_uuid = mapped_column(
        PGUUID,
        ForeignKey("metadata.tables.table_uuid"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    referenced_csvbase_row_id = mapped_column(
        satypes.BigInteger, nullable=False, index=True, primary_key=True
    )

    comment_obj = relationship("Comment", foreign_keys=[thread_id, comment_id])
    table_obj = relationship("Table")


# class BlogThread(Base):
#     __tablename__ = "blog_threads"

#     __table_args__ = (
#         METADATA_SCHEMA_TABLE_ARG,
#     )

#     thread_id = Column(
#         satypes.BigInteger,
#         ForeignKey("metadata.threads.thread_id"),
#         nullable=False,
#         index=True,
#     )

#     # this is not a foreign key because the csvbase-blog table is userdata
#     blogpost_id = Column(
#         satypes.BigInteger,
#         nullable=False,
#         index=True,
#     )


# class CommentReference(Base):
#     __tablename__ = "comment_references"
#     __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

#     reference_id = mapped_column(satypes.BigInteger, Identity(), primary_key=True)
#     comment_id = mapped_column(
#         satypes.BigInteger, ForeignKey("metadata.comments.comment_id"), nullable=False
#     )
#     updated = mapped_column(
#         satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
#     )
#     reference = mapped_column(satypes.JSON, nullable=False)


# class TableLogEntryEvents(Base):
#     __tablename__ = "table_log_events"
#     __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

#     event_type_id = mapped_column(satypes.SmallInteger, primary_key=True, autoincrement=False)
#     event_type_code = mapped_column(satypes.String, nullable=False)


# class TableLogEntry(Base):
#     __tablename__ = "table_log"
#     __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

#     table_log_id = mapped_column(satypes.BigInteger, Identity(), primary_key=True)
#     timestamp = mapped_column(
#         satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
#     )
#     event_type_id = mapped_column(
#         satypes.SmallInteger,
#         ForeignKey("metadata.table_log_events.event_type_id"),
#         nullable=False,
#         index=True,
#     )
#     event_body = mapped_column(satypes.JSON, nullable=False)


class CeleryScheduleEntry(Base):
    __tablename__ = "schedule_entries"
    __table_args__ = {"schema": "celery"}

    celery_app_name = mapped_column(satypes.String, primary_key=True, nullable=False)
    name = mapped_column(satypes.String, primary_key=True, nullable=False)
    created = mapped_column(satypes.DateTime(timezone=True), nullable=False)
    updated = mapped_column(satypes.DateTime(timezone=True), nullable=False)
    pickled_schedule_entry = mapped_column(satypes.PickleType(), nullable=False)
