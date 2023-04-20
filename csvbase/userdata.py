import csv
from typing import TYPE_CHECKING, Iterable, List, Optional, Sequence, Type, Collection
from uuid import UUID, uuid4

from pgcopy import CopyManager
from sqlalchemy import column as sacolumn, func, types as satypes, tuple_ as satuple
from sqlalchemy.orm import Session
from sqlalchemy.schema import Column as SAColumn, DDLElement
from sqlalchemy.schema import CreateTable, DropTable, MetaData, Identity  # type: ignore
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql.expression import (
    TableClause,
    select,
    table as satable,
    text,
    Executable,
    ClauseElement,
)
from sqlalchemy.ext.compiler import compiles

from . import conv
from .value_objs import (
    RowCount,
    Column,
    ColumnType,
    KeySet,
    Page,
    PythonType,
    Row,
    Table,
)
from .streams import UserSubmittedCSVData

if TYPE_CHECKING:
    from sqlalchemy.engine import RowProxy


class PGUserdataAdapter:
    @staticmethod
    def _make_temp_table_name(prefix: str) -> str:
        # FIXME: this name should probably include the date and some other helpful
        # info for debugging
        return f"{prefix}_{uuid4().hex}"

    @staticmethod
    def _get_tableclause(
        table_name: str, columns: Sequence[Column], schema: Optional[str] = None
    ) -> TableClause:
        return satable(  # type: ignore
            table_name,
            *[sacolumn(c.name, type_=c.type_.sqla_type()) for c in columns],
            schema=schema,
        )

    @classmethod
    def _get_userdata_tableclause(cls, sesh: Session, table_uuid: UUID) -> TableClause:
        columns = cls.get_columns(sesh, table_uuid)
        table_name = cls._make_userdata_table_name(table_uuid)
        return cls._get_tableclause(table_name, columns, schema="userdata")

    @staticmethod
    def _make_userdata_table_name(table_uuid: UUID, with_schema=False) -> str:
        if with_schema:
            return f"userdata.table_{table_uuid.hex}"
        else:
            return f"table_{table_uuid.hex}"

    @classmethod
    def count(cls, sesh: Session, table_uuid: UUID) -> RowCount:
        """Count the rows."""
        # we don't need the columns here, just a table
        tableclause = satable(cls._make_userdata_table_name(table_uuid), *[], schema="userdata")  # type: ignore
        exact, approx = sesh.execute(RowCountStatement(tableclause)).fetchone()
        return RowCount(exact, approx)

    @classmethod
    def get_columns(cls, sesh: Session, table_uuid: UUID) -> List["Column"]:
        # lifted from https://dba.stackexchange.com/a/22420/28877
        attrelid = cls._make_userdata_table_name(table_uuid, with_schema=True)
        stmt = text(
            """
        SELECT attname AS column_name, atttypid::regtype AS sql_type
        FROM   pg_attribute
        WHERE  attrelid = :table_name ::regclass
        AND    attnum > 0
        AND    NOT attisdropped
        ORDER  BY attnum
        """
        )
        rs = sesh.execute(stmt, {"table_name": attrelid})
        rv = []
        for name, sql_type in rs:
            rv.append(Column(name=name, type_=ColumnType.from_sql_type(sql_type)))
        return rv

    @classmethod
    def get_row(cls, sesh: Session, table_uuid: UUID, row_id: int) -> Optional[Row]:
        columns = cls.get_columns(sesh, table_uuid)
        table_clause = cls._get_userdata_tableclause(sesh, table_uuid)
        cursor = sesh.execute(
            table_clause.select().where(table_clause.c.csvbase_row_id == row_id)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return {c: row[c.name] for c in columns}

    @classmethod
    def min_row_id(cls, sesh: Session, table_uuid: UUID) -> int:
        """Returns the lowest row id in the table, or if there are no rows, 0."""
        table_clause = cls._get_userdata_tableclause(sesh, table_uuid)
        stmt = (
            select([table_clause.c.csvbase_row_id])
            .order_by(table_clause.c.csvbase_row_id)
            .limit(1)
        )
        cursor = sesh.execute(stmt)
        row_id: Optional[int] = cursor.scalar()
        if row_id is None:
            return 0
        else:
            return row_id

    @classmethod
    def get_a_sample_row(cls, sesh: Session, table_uuid: UUID) -> Row:
        """Returns a sample row from the table (the lowest row id).

        If none exist, a made-up row is returned.  This function is for
        example/documentation purposes only."""
        columns = cls.get_columns(sesh, table_uuid)
        table_clause = cls._get_userdata_tableclause(sesh, table_uuid)
        cursor = sesh.execute(table_clause.select().order_by("csvbase_row_id").limit(1))
        row = cursor.fetchone()
        if row is None:
            # return something made-up
            return {c: c.type_.example() for c in columns}
        else:
            return {c: row[c.name] for c in columns}

    @classmethod
    def insert_row(cls, sesh: Session, table_uuid: UUID, row: Row) -> int:
        table = cls._get_userdata_tableclause(sesh, table_uuid)
        values = {c.name: v for c, v in row.items()}
        return sesh.execute(
            table.insert().values(values).returning(table.c.csvbase_row_id)
        ).scalar()

    @classmethod
    def update_row(
        cls,
        sesh: Session,
        table_uuid: UUID,
        row_id: int,
        row: Row,
    ) -> bool:
        """Update a given row, returning True if it existed (and was updated) and False otherwise."""
        table = cls._get_userdata_tableclause(sesh, table_uuid)
        values = {c.name: v for c, v in row.items()}
        result = sesh.execute(
            table.update().where(table.c.csvbase_row_id == row_id).values(values)
        )
        return result.rowcount > 0

    @classmethod
    def delete_row(cls, sesh: Session, table_uuid: UUID, row_id: int) -> bool:
        """Update a given row, returning True if it existed (and was updated) and False otherwise."""
        table = cls._get_userdata_tableclause(sesh, table_uuid)
        result = sesh.execute(table.delete().where(table.c.csvbase_row_id == row_id))
        return result.rowcount > 0

    @classmethod
    def table_page(cls, sesh: Session, table: Table, keyset: KeySet) -> Page:
        """Get a page from a table based on the provided KeySet"""
        table_clause = cls._get_userdata_tableclause(sesh, table.table_uuid)
        key_names = [col.name for col in keyset.columns]
        table_vals = satuple(
            table_clause.c.csvbase_row_id,
        )
        keyset_vals = satuple(*keyset.values)
        if keyset.op == "greater_than":
            where_cond = table_vals > keyset_vals
        else:
            where_cond = table_vals < keyset_vals

        keyset_page = table_clause.select().where(where_cond).limit(keyset.size)

        if keyset.op == "greater_than":
            keyset_page = keyset_page.order_by(table_vals)
        else:
            # if we're going backwards we need to reverse the order via a subquery
            keyset_page = keyset_page.order_by(table_vals.desc())
            keyset_sub = select(keyset_page.alias())  # type: ignore
            keyset_page = keyset_sub.order_by(*key_names)

        row_tuples: List[RowProxy] = list(sesh.execute(keyset_page))

        if len(row_tuples) > 1:
            first_row = row_tuples[0]
            last_row = row_tuples[-1]

            has_more_vals = satuple(
                *[getattr(last_row, colname) for colname in key_names]
            )
            has_more_q = (
                table_clause.select().where(table_vals > has_more_vals).exists()  # type: ignore
            )
            has_more = sesh.query(has_more_q).scalar()
            has_less_vals = satuple(
                *[getattr(first_row, colname) for colname in key_names]
            )
            has_less_q = (
                table_clause.select().where(table_vals < has_less_vals).exists()  # type: ignore
            )
            has_less = sesh.query(has_less_q).scalar()
        else:
            if keyset.op == "greater_than":
                has_more = False
                has_less = sesh.query(
                    table_clause.select().where(table_vals < keyset_vals).exists()  # type: ignore
                ).scalar()
            else:
                has_more = sesh.query(
                    table_clause.select().where(table_vals > keyset_vals).exists()  # type: ignore
                ).scalar()
                has_less = False

        rows = [{c: row_tup[c.name] for c in table.columns} for row_tup in row_tuples]

        return Page(
            has_less=has_less,
            has_more=has_more,
            rows=rows[: keyset.size],
        )

    @classmethod
    def table_as_rows(
        cls,
        sesh: Session,
        table_uuid: UUID,
    ) -> Iterable[Sequence[PythonType]]:
        table_clause = cls._get_userdata_tableclause(sesh, table_uuid)
        columns = cls.get_columns(sesh, table_uuid)
        q = select([getattr(table_clause.c, c.name) for c in columns]).order_by(
            table_clause.c.csvbase_row_id
        )
        yield from sesh.execute(q)

    @classmethod
    def insert_table_data(
        cls,
        sesh: Session,
        table: Table,
        columns: Sequence[Column],
        rows: Iterable[Sequence[PythonType]],
    ) -> None:
        temp_table_name = cls._make_temp_table_name(prefix="insert")
        main_table_name = cls._make_userdata_table_name(
            table.table_uuid, with_schema=True
        )
        main_tableclause = cls._get_userdata_tableclause(sesh, table.table_uuid)
        sesh.execute(CreateTempTableLike(satable(temp_table_name), main_tableclause))

        raw_conn = sesh.connection().connection
        column_names = [c.name for c in columns]
        copy_manager = CopyManager(
            raw_conn,
            temp_table_name,
            column_names,
        )
        copy_manager.copy(rows)

        temp_tableclause = cls._get_tableclause(temp_table_name, table.columns)

        add_stmt_select_columns = [getattr(temp_tableclause.c, c) for c in column_names]
        add_stmt_no_blanks = main_tableclause.insert().from_select(
            column_names,
            select(*add_stmt_select_columns)
            .select_from(temp_tableclause)
            .where(temp_tableclause.c.csvbase_row_id.is_not(None)),  # type: ignore
        )

        reset_serial_stmt = select(
            func.setval(
                func.pg_get_serial_sequence(main_table_name, "csvbase_row_id"),
                func.max(main_tableclause.c.csvbase_row_id),
            )
        )

        select_columns = [
            func.coalesce(
                func.nextval(
                    func.pg_get_serial_sequence(main_table_name, "csvbase_row_id")
                )
            )
        ]
        select_columns += [
            getattr(temp_tableclause.c, c.name) for c in table.user_columns()
        ]
        add_stmt_blanks = main_tableclause.insert().from_select(
            [c.name for c in table.columns],
            select(*select_columns)
            .select_from(temp_tableclause)
            .where(
                temp_tableclause.c.csvbase_row_id.is_(None),
            ),
        )

        sesh.execute(add_stmt_no_blanks)
        sesh.execute(reset_serial_stmt)
        sesh.execute(add_stmt_blanks)

    @classmethod
    def drop_table(cls, sesh: Session, table_uuid: UUID) -> None:
        sa_table = cls._get_userdata_tableclause(sesh, table_uuid)
        sesh.execute(DropTable(sa_table))  # type: ignore

    @classmethod
    def create_table(
        cls, sesh: Session, table_uuid: UUID, columns: Iterable[Column]
    ) -> UUID:
        cols: List[SAColumn] = [
            SAColumn(
                "csvbase_row_id", satypes.BigInteger, Identity(), primary_key=True
            ),
            # FIXME: would be good to have these two columns plus
            # "csvbase_created_by" and csvbase_updated_by, but needs support for
            # datetimes as a type
            # SAColumn(
            #     "csvbase_created",
            #     type_=satypes.TIMESTAMP(timezone=True),
            #     nullable=False,
            #     default="now()",
            # ),
            # SAColumn(
            #     "csvbase_update",
            #     type_=satypes.TIMESTAMP(timezone=True),
            #     nullable=False,
            #     default="now()",
            # ),
        ]
        for col in columns:
            # Don't create 'csvbase_'-prefixed columns from user data, we
            # control those.
            if not col.name.startswith("csvbase_"):
                cols.append(SAColumn(col.name, type_=col.type_.sqla_type()))
        table = SATable(
            cls._make_userdata_table_name(table_uuid),
            MetaData(),
            *cols,
            schema="userdata",
        )
        sesh.execute(CreateTable(table))
        return table_uuid

    @classmethod
    def upsert_table_data(
        cls, sesh: Session, table: Table, row_columns: Sequence[Column], rows: Iterable
    ) -> None:
        """Upsert table data from rows into the SQL table.

        Note that the columns being upserted can be a subset of the columns
        that are present in the SQL table.  If the csvbase_row_id column is
        present, it will be used to correlate changes.

        """
        # First, make a temp table and COPY the new rows into it
        temp_table_name = cls._make_temp_table_name(prefix="upsert")
        main_table_name = cls._make_userdata_table_name(
            table.table_uuid, with_schema=True
        )
        main_tableclause = cls._get_userdata_tableclause(sesh, table.table_uuid)
        sesh.execute(CreateTempTableLike(satable(temp_table_name), main_tableclause))
        raw_conn = sesh.connection().connection
        upsert_column_names = [c.name for c in row_columns]
        existing_column_names = [c.name for c in table.columns]
        copy_manager = CopyManager(raw_conn, temp_table_name, upsert_column_names)
        copy_manager.copy(rows)

        # Next selectively use the temp table to update the 'main' one
        temp_tableclause = cls._get_tableclause(temp_table_name, table.columns)

        # 1. for removals
        remove_stmt = main_tableclause.delete().where(
            main_tableclause.c.csvbase_row_id.not_in(  # type: ignore
                select(temp_tableclause.c.csvbase_row_id)  # type: ignore
            )
        )

        # 2. updates
        update_stmt = (
            main_tableclause.update()
            .values(
                **{
                    col.name: getattr(temp_tableclause.c, col.name)
                    for col in table.columns
                }
            )
            .where(
                main_tableclause.c.csvbase_row_id == temp_tableclause.c.csvbase_row_id
            )
        )

        # 3a. and additions where the csvbase_row_id as been set
        add_stmt_select_columns = [
            getattr(temp_tableclause.c, c.name) for c in table.columns
        ]
        add_stmt_no_blanks = main_tableclause.insert().from_select(
            existing_column_names,
            select(*add_stmt_select_columns)  # type: ignore
            .select_from(
                temp_tableclause.outerjoin(
                    main_tableclause,
                    main_tableclause.c.csvbase_row_id
                    == temp_tableclause.c.csvbase_row_id,
                )
            )
            .where(
                main_tableclause.c.csvbase_row_id.is_(None),
                temp_tableclause.c.csvbase_row_id.is_not(None),  # type: ignore
            ),
        )

        # 3b. now reseting the sequence that allocates pks
        reset_serial_stmt = select(
            func.setval(
                func.pg_get_serial_sequence(main_table_name, "csvbase_row_id"),
                func.max(main_tableclause.c.csvbase_row_id),
            )
        )

        # 3c. additions which do not have a csvbase_row_id set
        select_columns = [
            func.coalesce(
                func.nextval(
                    func.pg_get_serial_sequence(main_table_name, "csvbase_row_id")
                )
            )
        ]
        select_columns += [
            getattr(temp_tableclause.c, c.name) for c in table.user_columns()
        ]
        add_stmt_blanks = main_tableclause.insert().from_select(
            existing_column_names,
            select(*select_columns)  # type: ignore
            .select_from(
                temp_tableclause.outerjoin(
                    main_tableclause,
                    main_tableclause.c.csvbase_row_id
                    == temp_tableclause.c.csvbase_row_id,
                )
            )
            .where(
                main_tableclause.c.csvbase_row_id.is_(None),
                temp_tableclause.c.csvbase_row_id.is_(None),
            ),
        )

        sesh.execute(remove_stmt)
        sesh.execute(update_stmt)
        sesh.execute(add_stmt_no_blanks)
        sesh.execute(reset_serial_stmt)
        sesh.execute(add_stmt_blanks)

    @classmethod
    def byte_count(cls, sesh: Session, table_uuid: UUID) -> int:
        # pg_total_relation_size returns the size of the table plus toast, plus
        # indices https://stackoverflow.com/a/70397779
        stmt = text("SELECT pg_total_relation_size(:relname);")
        rs = sesh.execute(
            stmt,
            params={
                "relname": cls._make_userdata_table_name(table_uuid, with_schema=True)
            },
        )
        return rs.scalar()


class CreateTempTableLike(DDLElement):
    inherit_cache = False

    def __init__(self, temp_table: TableClause, like_table: TableClause):
        self.temp_table = temp_table
        self.like_table = like_table


@compiles(CreateTempTableLike, "postgresql")
def visit_create_temp_table(element, compiler, **kw):
    # We use CREATE TABLE AS instead of CREATE TABLE LIKE because the latter
    # always copies not null constraints, which prevents us from autogenerating
    # csvbase_row_ids where necessary
    stmt = "CREATE TEMP TABLE %s ON COMMIT DROP AS SELECT * FROM %s LIMIT 0"
    return stmt % (
        element.temp_table,
        element.like_table,
    )


class RowCountStatement(Executable, ClauseElement):
    inherit_cache = False

    def __init__(self, table: TableClause):
        self.table = table


@compiles(RowCountStatement, "postgresql")
def visit_rowcount(element, compiler, **kw):
    # In PG, exact counts are slow, so for bigger tables, do an approximate
    # count.
    stmt = "SELECT count(*), count(*) from %s"
    return stmt % compiler.process(element.table, asfrom=True, **kw)
