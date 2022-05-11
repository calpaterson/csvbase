from uuid import UUID, uuid4
from typing import List
from typing import TYPE_CHECKING

from sqlalchemy.dialects.postgresql import UUID as _PGUUID, BYTEA
from sqlalchemy import (
    Column,
    ForeignKey,
    types as satypes,
    UniqueConstraint,
    func,
)
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.schema import CheckConstraint, MetaData, Identity  # type: ignore
from sqlalchemy.orm import RelationshipProperty, relationship

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


METADATA_SCHEMA_TABLE_ARG = {"schema": "metadata"}


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("username ~ '^[A-z][-A-z0-9]+$'", "username_format"),
        CheckConstraint("char_length(username) <= 200", "username_length"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    user_uuid = Column(PGUUID, primary_key=True)
    username = Column(satypes.String, nullable=False, unique=True, index=True)
    password_hash = Column(satypes.String, nullable=False)
    timezone = Column(satypes.String, nullable=False)
    registered = Column(satypes.DateTime(timezone=True), nullable=False)

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


class UserEmail(Base):
    __tablename__ = "user_emails"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = Column(PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True)
    email_address = Column(satypes.String(length=200), nullable=False, index=True)


class APIKey(Base):
    __tablename__ = "api_keys"
    __table_args__ = (METADATA_SCHEMA_TABLE_ARG,)

    user_uuid = Column(PGUUID, ForeignKey("metadata.users.user_uuid"), primary_key=True)
    api_key = Column(BYTEA(length=16), nullable=False, unique=True, index=True)


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

    readme_obj: "RelationshipProperty[TableReadme]" = relationship(
        "TableReadme", uselist=False, backref="table"
    )

    praise_objs: "RelationshipProperty[List[Praise]]" = relationship(
        "Praise", uselist=True, backref="table_objs"
    )


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
