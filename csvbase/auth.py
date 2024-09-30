"""Work in progress alternate permissions implementation"""

from enum import Enum
from typing import Union
from typing_extensions import Literal
from uuid import UUID

from sqlalchemy import types as satypes
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import select, literal_column, union_all

from csvbase.value_objs import Table
from csvbase import models
from csvbase.web.func import get_current_user


class ObjectType(Enum):
    TABLE = 1
    COMMENT = 2

    def as_sql(self):
        return literal_column(str(self.value), satypes.SmallInteger)


class ActionType(Enum):
    READ = 1
    WRITE = 2
    ADMIN = 3

    def as_sql(self):
        return literal_column(str(self.value), satypes.SmallInteger)


def _build_table_permissions_subselect(sesh: Session, user_uuid: UUID):
    """Return all the table permissions that the given user has (as a query)."""
    public_table_permissions = select(
        ObjectType.TABLE.as_sql().label("object"),
        models.Table.table_uuid,
        ActionType.READ.as_sql().label("action"),
    ).where(models.Table.public)
    private_table_permissions = select(
        ObjectType.TABLE.as_sql().label("object"),
        models.Table.table_uuid,
        ActionType.WRITE.as_sql().label("action"),
    ).where(models.Table.user_uuid == user_uuid)

    return union_all(private_table_permissions, public_table_permissions)


def ensure_table_access(
    sesh: Session, table: Table, mode: Union[Literal["read"], Literal["write"]]
) -> None:
    """Return happily if user is allowed to access the given table, raise otherwise."""
    current_user = get_current_user()
    action = ActionType.READ if mode == "read" else ActionType.WRITE

    # Users's current permissions
    table_permissions = _build_table_permissions_subselect(
        sesh, current_user.user_uuid  # type: ignore
    ).subquery()

    # Check that user has access to do what they are currently doing
    exists_stmt = (
        select(table_permissions)
        .where(
            table_permissions.c.object == ObjectType.TABLE.as_sql(),
            table_permissions.c.table_uuid == table.table_uuid,
            table_permissions.c.action == action.as_sql(),
        )
        .exists()
    )

    rv = sesh.execute(select(exists_stmt)).scalar()

    # If they do not have sufficient perms, raise
    if not rv:
        raise RuntimeError("not allowed")
