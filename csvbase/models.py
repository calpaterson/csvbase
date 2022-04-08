from uuid import UUID, uuid4
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
from sqlalchemy.schema import CheckConstraint
from sqlalchemy.orm import RelationshipProperty, relationship

Base = declarative_base()

# https://github.com/dropbox/sqlalchemy-stubs/issues/94
if TYPE_CHECKING:
    PGUUID = satypes.TypeEngine[UUID]
else:
    PGUUID = _PGUUID(as_uuid=True)


METADATA_SCHEMA_TABLE_ARG = {"schema": "metadata"}


class User(Base):
    __tablename__ = "users"
    __table_args__ = (
        CheckConstraint("username ~ '^[A-z][-A-z0-9]+$'"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    user_uuid = Column(PGUUID, primary_key=True)
    username = Column(
        satypes.String(length=200), nullable=False, unique=True, index=True
    )
    password_hash = Column(satypes.String, nullable=False)
    timezone = Column(satypes.String, nullable=False)
    registered = Column(satypes.DateTime(timezone=True), nullable=False)

    email_obj: "RelationshipProperty[UserEmail]" = relationship(
        "UserEmail", uselist=False, backref="user"
    )

    api_key: "RelationshipProperty[APIKey]" = relationship(
        "APIKey", uselist=False, backref="user"
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
        CheckConstraint("table_name ~ '^[A-z][-A-z0-9]+$'"),
        UniqueConstraint("user_uuid", "table_name"),
        METADATA_SCHEMA_TABLE_ARG,
    )

    table_uuid = Column(PGUUID, primary_key=True, default=uuid4)
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
    table_name = Column(satypes.String(length=200), nullable=False, index=True)
    caption = Column(
        satypes.String(length=200),
        nullable=False,
    )

    readme_obj: "RelationshipProperty[TableReadme]" = relationship(
        "TableReadme", uselist=False, backref="table"
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
