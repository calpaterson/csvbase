from typing import TYPE_CHECKING, List, Mapping
from uuid import UUID

from sqlalchemy import Column, ForeignKey, UniqueConstraint, func
from sqlalchemy import types as satypes
from sqlalchemy.dialects.postgresql import BYTEA
from sqlalchemy.dialects.postgresql import UUID as _PGUUID

if TYPE_CHECKING:
    from sqlalchemy.ext.declarative import declarative_base
else:
    from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import RelationshipProperty, relationship
from sqlalchemy.schema import CheckConstraint, Identity, MetaData  # type: ignore

naming_convention = {
    "ix": "ix_%(column_0_label)s",
    "uq": "uq_%(table_name)s_%(column_0_name)s",
    "ck": "ck_%(table_name)s_%(constraint_name)s",
    "fk": "fk_%(table_name)s_%(column_0_name)s_%(referred_table_name)s",
    "pk": "pk_%(table_name)s",
}


metadata = MetaData(naming_convention=naming_convention)

Base = declarative_base(metadata=metadata)

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

    user_uuid = Column(PGUUID, primary_key=True)
    username = Column(satypes.String, nullable=False, unique=True, index=True)
    password_hash = Column(satypes.String, nullable=False)
    timezone = Column(satypes.String, nullable=False)
    registered = Column(satypes.DateTime(timezone=True), nullable=False)
    mailing_list = Column(satypes.Boolean, nullable=False)

    email_obj: "RelationshipProperty[UserEmail]" = relationship(
        "UserEmail", uselist=False, backref="user"
    )

    api_key: "RelationshipProperty[APIKey]" = relationship(
        "APIKey", uselist=False, backref="user"
    )

    table_objs: "RelationshipProperty[List[Table]]" = relationship(
        "Table", uselist=True, backref="user"
    )

    praise_objs: "RelationshipProperty[List[Praise]]" = relationship(
        "Praise", uselist=True, backref="user_objs"
    )

    stripe_customers_obj: "RelationshipProperty[StripeCustomer]" = relationship(
        "StripeCustomer", uselist=False, backref="user_obj"
    )

    payment_references: "RelationshipProperty[List[PaymentReference]]" = relationship(
        "PaymentReference", uselist=True, backref="user_objs"
    )


class UserEmail(Base):
    __tablename__ = "user_emails"
    __table_args__ = (
        CheckConstraint("email_address ~ '@'", "email_address_not_blank"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    user_uuid = Column(PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True)
    email_address = Column(satypes.String(length=200), nullable=False, index=True)


class APIKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = Column(PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True)
    api_key = Column(BYTEA(length=16), nullable=False, unique=True, index=True)


class TableBackend(Base):
    __tablename__ = "table_backends"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    backend_id = Column(satypes.SmallInteger, primary_key=True, autoincrement=False)
    backend_name = Column(satypes.String, nullable=False)


class Table(Base):
    __tablename__ = "tables"
    __table_args__ = (
        CheckConstraint("table_name ~ '^[A-z][-A-z0-9]+$'", "table_name_format"),
        CheckConstraint("char_length(table_name) <= 200", "table_name_length"),
        CheckConstraint("char_length(caption) <= 200", "caption_length"),
        UniqueConstraint("user_uuid", "table_name"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    table_uuid = Column(PGUUID, primary_key=True)
    user_uuid = Column(PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False)
    public = Column(satypes.Boolean, nullable=False)
    created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    licence_id = Column(
        satypes.SmallInteger,
        ForeignKey("metadata.data_licences.licence_id"),
        nullable=False,
    )
    table_name = Column(satypes.String, nullable=False, index=True)
    caption = Column(
        satypes.String,
        nullable=False,
    )
    last_changed = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    backend_id = Column(
        satypes.SmallInteger, ForeignKey(TableBackend.backend_id), nullable=False
    )

    readme_obj: "RelationshipProperty[TableReadme]" = relationship(
        "TableReadme", uselist=False, backref="table"
    )

    praise_objs: "RelationshipProperty[List[Praise]]" = relationship(
        "Praise", uselist=True, backref="table_objs"
    )
    backend_obj: "RelationshipProperty[TableBackend]" = relationship(
        "TableBackend", uselist=False, backref="table_objs"
    )
    git_upstream_obj: "RelationshipProperty[GitUpstream]" = relationship(
        "GitUpstream", uselist=False, backref="table_objs"
    )


class UniqueColumn(Base):
    __tablename__ = "unique_columns"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    table_uuid = Column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), primary_key=True
    )
    column_name = Column(satypes.String, nullable=False, primary_key=True)


class GitUpstream(Base):
    # FIXME: table should be called "git_upstreams"
    __tablename__ = "github_follows"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    table_uuid = Column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), primary_key=True
    )
    last_sha = Column(BYTEA, nullable=False)
    last_modified = Column(satypes.DateTime(timezone=True), nullable=False)
    https_repo_url = Column(satypes.String, nullable=False)
    branch = Column(satypes.String, nullable=False)
    path = Column(satypes.String, nullable=False)
    # FIXME: to come
    # last_checked = Column(satypes.DateTime(timezone=True), nullable=False)


class TableReadme(Base):
    __tablename__ = "table_readmes"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    table_uuid = Column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), primary_key=True
    )
    readme_markdown = Column(satypes.String(length=10_000), nullable=False)


class DataLicence(Base):
    __tablename__ = "data_licences"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    licence_id = Column(satypes.SmallInteger, primary_key=True, autoincrement=False)
    # FIXME: name should be nullable = false
    licence_name = Column(satypes.String)


class Praise(Base):
    __tablename__ = "praise"
    __table_args__ = (
        UniqueConstraint("user_uuid", "table_uuid"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    praise_id = Column(satypes.BigInteger, Identity(), primary_key=True)
    table_uuid = Column(
        PGUUID, ForeignKey("metadata.tables.table_uuid"), nullable=False, index=True
    )
    user_uuid = Column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False, index=True
    )
    praised = Column(
        satypes.TIMESTAMP(timezone=True),
        nullable=False,
        index=True,
        server_default=func.current_timestamp(),
    )


class ProhibitedUsername(Base):
    __tablename__ = "prohibited_usernames"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    username = Column(satypes.String, nullable=False, primary_key=True)


class PaymentReference(Base):
    __tablename__ = "payment_references"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    payment_reference_uuid = Column(PGUUID, primary_key=True)
    user_uuid = Column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), index=True, nullable=False
    )
    payment_reference = Column(satypes.String, index=True, unique=True, nullable=False)
    created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )


class StripeCustomer(Base):
    __tablename__ = "stripe_customers"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = Column(
        PGUUID,
        ForeignKey("metadata.users.user_uuid"),
        primary_key=True,
    )
    stripe_customer_id = Column(satypes.String, nullable=False, index=True, unique=True)
    created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )


class StripeSubscription(Base):
    __tablename__ = "stripe_subscriptions"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    stripe_subscription_id = Column(satypes.String, primary_key=True)
    user_uuid = Column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False, index=True
    )
    stripe_subscription_status_id = Column(
        satypes.SmallInteger,
        ForeignKey(
            "metadata.stripe_subscription_statuses.stripe_subscription_status_id"
        ),
        index=True,
        nullable=False,
    )
    created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    updated = Column(
        satypes.DateTime(timezone=True),
        default=func.now(),
        nullable=False,
        index=True,
        onupdate=func.now(),
    )
    ttl = Column(satypes.DateTime(timezone=True), nullable=False, index=True)


class StripeSubscriptionStatus(Base):
    __tablename__ = "stripe_subscription_statuses"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    stripe_subscription_status_id = Column(
        satypes.SmallInteger, primary_key=True, unique=True, autoincrement=False
    )
    stripe_subscription_status = Column(satypes.String, nullable=False, unique=True)


class Copy(Base):
    __tablename__ = "copies"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    copy_id = Column(satypes.BigInteger, Identity(), unique=True, index=True)
    from_uuid = Column(
        PGUUID,
        ForeignKey("metadata.tables.table_uuid"),
        nullable=False,
        index=True,
        primary_key=True,
    )
    to_uuid = Column(
        PGUUID,
        ForeignKey("metadata.tables.table_uuid"),
        nullable=False,
        index=True,
        primary_key=True,
    )

    created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )

    from_obj: "RelationshipProperty[Table]" = relationship(
        "Table",
        uselist=False,
        foreign_keys=[from_uuid],
    )
    to_obj: "RelationshipProperty[Table]" = relationship(
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

    thread_id = Column(satypes.BigInteger, Identity(), primary_key=True)
    thread_created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    thread_updated = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    user_uuid = Column(PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False)
    thread_title = Column(satypes.String, nullable=False)


class Comment(Base):
    __tablename__ = "comments"
    __table_args__ = (
        CheckConstraint("char_length(comment_markdown) <= 4000", "comments_markdown_length"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    comment_id = Column(satypes.BigInteger, Identity(), primary_key=True)
    user_uuid = Column(
        PGUUID, ForeignKey("metadata.users.user_uuid"), nullable=False, index=True
    )
    thread_id = Column(
        satypes.BigInteger,
        ForeignKey("metadata.threads.thread_id"),
        nullable=False,
        index=True,
    )
    comment_created = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    comment_updated = Column(
        satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
    )
    comment_markdown = Column(satypes.String, nullable=False)


# class CommentReference(Base):
#     __tablename__ = "comment_references"
#     __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

#     reference_id = Column(satypes.BigInteger, Identity(), primary_key=True)
#     comment_id = Column(
#         satypes.BigInteger, ForeignKey("metadata.comments.comment_id"), nullable=False
#     )
#     updated = Column(
#         satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
#     )
#     reference = Column(satypes.JSON, nullable=False)


# class TableLogEntryEvents(Base):
#     __tablename__ = "table_log_events"
#     __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

#     event_type_id = Column(satypes.SmallInteger, primary_key=True, autoincrement=False)
#     event_type_code = Column(satypes.String, nullable=False)


# class TableLogEntry(Base):
#     __tablename__ = "table_log"
#     __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

#     table_log_id = Column(satypes.BigInteger, Identity(), primary_key=True)
#     timestamp = Column(
#         satypes.DateTime(timezone=True), default=func.now(), nullable=False, index=True
#     )
#     event_type_id = Column(
#         satypes.SmallInteger,
#         ForeignKey("metadata.table_log_events.event_type_id"),
#         nullable=False,
#         index=True,
#     )
#     event_body = Column(satypes.JSON, nullable=False)
