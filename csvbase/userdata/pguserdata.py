from typing import (
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
)
from uuid import UUID, uuid4

from pgcopy import CopyManager
from sqlalchemy import (
    column as sacolumn,
    func,
    types as satypes,
    tuple_ as satuple,
    delete,
    and_,
    ColumnClause,
)
from sqlalchemy.orm import Session
from sqlalchemy.schema import Column as SAColumn, DDLElement
from sqlalchemy.schema import CreateTable, DropTable, MetaData, Identity
from sqlalchemy.schema import Table as SATable
from sqlalchemy.sql.expression import (
    TableClause,
    select,
    table as satable,
    text,
    Executable,
    ClauseElement,
)
from sqlalchemy.sql.dml import ReturningInsert
from sqlalchemy.ext.compiler import compiles

from ..value_objs import (
    RowCount,
    Column,
    ColumnType,
    KeySet,
    Page,
    PythonType,
    Row,
    Table,
    ROW_ID_COLUMN,
)


class PGUserdataAdapter:
    def __init__(self, sesh: Session) -> None:
        self.sesh = sesh

    # NOTE: Could be a function
    def _make_temp_table_name(self, prefix: str) -> str:
        # FIXME: this name should probably include the date and some other helpful
        # info for debugging
        return f"{prefix}_{uuid4().hex}"

    # NOTE: Could be a function
    def _get_tableclause(
        self, table_name: str, columns: Sequence[Column], schema: Optional[str] = None
    ) -> TableClause:
        return satable(
            table_name,
            *[sacolumn(c.name, type_=c.type_.sqla_type()) for c in columns],
            schema=schema,
        )

    def _get_userdata_tableclause(self, table_uuid: UUID) -> TableClause:
        columns = self.get_columns(table_uuid)
        table_name = self._make_userdata_table_name(table_uuid)
        return self._get_tableclause(table_name, columns, schema="userdata")

    # NOTE: Could be a function
    def _make_userdata_table_name(self, table_uuid: UUID, with_schema=False) -> str:
        if with_schema:
            return f"userdata.table_{table_uuid.hex}"
        else:
            return f"table_{table_uuid.hex}"

    def _reset_pk_sequence(self, tableclause: TableClause) -> None:
        """Reset the csvbase_row_id sequence to the max of the column.

        This should be done after inserts that raise the csvbase_row_id.

        """
        fullname = tableclause.fullname

        stmt = select(
            func.setval(
                func.pg_get_serial_sequence(fullname, "csvbase_row_id"),
                func.greatest(
                    func.max(tableclause.c.csvbase_row_id),
                    # the below awful hack is required because currval is only
                    # for when the current value is already in the session
                    func.greatest(
                        func.nextval(
                            func.pg_get_serial_sequence(fullname, "csvbase_row_id")
                        )
                        - 1,
                        1,
                    ),
                ),
            )
        )
        self.sesh.execute(stmt)

    def count(self, table_uuid: UUID) -> RowCount:
        """Count the rows."""
        # we don't need the columns here, just a table
        tableclause = satable(
            self._make_userdata_table_name(table_uuid), *[], schema="userdata"
        )
        exact, approx = cast(
            Tuple[Optional[int], int],
            self.sesh.execute(RowCountStatement(tableclause)).fetchone(),
        )
        return RowCount(exact, approx)

    def get_columns(self, table_uuid: UUID) -> List["Column"]:
        # lifted from https://dba.stackexchange.com/a/22420/28877
        attrelid = self._make_userdata_table_name(table_uuid, with_schema=True)
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
        rs = self.sesh.execute(stmt, {"table_name": attrelid})
        rv = []
        for name, sql_type in rs:
            rv.append(Column(name=name, type_=ColumnType.from_sql_type(sql_type)))
        return rv

    def get_row(self, table_uuid: UUID, row_id: int) -> Optional[Row]:
        columns = self.get_columns(table_uuid)
        table_clause = self._get_userdata_tableclause(table_uuid)
        cursor = self.sesh.execute(
            table_clause.select().where(table_clause.c.csvbase_row_id == row_id)
        )
        row = cursor.fetchone()
        if row is None:
            return None
        else:
            return {c: row._mapping[c.name] for c in columns}

    def min_row_id(self, table_uuid: UUID) -> int:
        """Returns the lowest row id in the table, or if there are no rows, 0."""
        table_clause = self._get_userdata_tableclause(table_uuid)
        row_id: ColumnClause[int] = table_clause.c.csvbase_row_id
        stmt = select(row_id).order_by(row_id).limit(1)
        cursor = self.sesh.execute(stmt)
        min_rid: Optional[int] = cursor.scalar()
        if min_rid is None:
            return 0
        else:
            return min_rid

    def row_id_bounds(self, table_uuid: UUID) -> Tuple[Optional[int], Optional[int]]:
        """Returns the min and max row id.

        These may be negative if the user has used negative ids.

        An empty table will return (None, None).

        """
        table_clause = self._get_userdata_tableclause(table_uuid)
        stmt = select(
            func.min(table_clause.c.csvbase_row_id),
            func.max(table_clause.c.csvbase_row_id),
        )
        cursor = self.sesh.execute(stmt)
        return cast(Tuple[Optional[int], Optional[int]], cursor.fetchone())

    def get_a_sample_row(self, table_uuid: UUID) -> Row:
        """Returns a sample row from the table (the lowest row id).

        If none exist, a made-up row is returned.  This function is for
        example/documentation purposes only."""
        columns = self.get_columns(table_uuid)
        table_clause = self._get_userdata_tableclause(table_uuid)
        cursor = self.sesh.execute(
            table_clause.select().order_by("csvbase_row_id").limit(1)
        )
        row = cursor.fetchone()
        if row is None:
            # return something made-up
            return {c: c.type_.example() for c in columns}
        else:
            return {c: row._mapping[c.name] for c in columns}

    def insert_row(self, table_uuid: UUID, row: Row) -> int:
        table = self._get_userdata_tableclause(table_uuid)
        values = {c.name: v for c, v in row.items()}
        stmt: ReturningInsert[Tuple[int]] = (
            table.insert().values(values).returning(table.c.csvbase_row_id)
        )
        return cast(int, self.sesh.execute(stmt).scalar())

    def update_row(
        self,
        table_uuid: UUID,
        row_id: int,
        row: Row,
    ) -> bool:
        """Update a given row, returning True if it existed (and was updated) and False otherwise."""
        table = self._get_userdata_tableclause(table_uuid)
        values = {c.name: v for c, v in row.items()}
        result = self.sesh.execute(
            table.update().where(table.c.csvbase_row_id == row_id).values(values)
        )
        return result.rowcount > 0

    def delete_row(self, table_uuid: UUID, row_id: int) -> bool:
        """Update a given row, returning True if it existed (and was updated) and False otherwise."""
        table = self._get_userdata_tableclause(table_uuid)
        result = self.sesh.execute(
            table.delete().where(table.c.csvbase_row_id == row_id)
        )
        return result.rowcount > 0

    def table_page(self, table: Table, keyset: KeySet) -> Page:
        """Get a page from a table based on the provided KeySet"""
        table_clause = self._get_userdata_tableclause(table.table_uuid)
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
            keyset_sub = select(keyset_page.alias())
            keyset_page = keyset_sub.order_by(*key_names)

        row_tuples = list(self.sesh.execute(keyset_page))

        if len(row_tuples) > 1:
            first_row = row_tuples[0]
            last_row = row_tuples[-1]

            has_more_vals = satuple(
                *[getattr(last_row, colname) for colname in key_names]
            )
            has_more_q = (
                table_clause.select().where(table_vals > has_more_vals).exists()
            )
            has_more = self.sesh.query(has_more_q).scalar()
            has_less_vals = satuple(
                *[getattr(first_row, colname) for colname in key_names]
            )
            has_less_q = (
                table_clause.select().where(table_vals < has_less_vals).exists()
            )
            has_less = self.sesh.query(has_less_q).scalar()
        else:
            if keyset.op == "greater_than":
                has_more = False
                has_less = self.sesh.query(
                    table_clause.select().where(table_vals < keyset_vals).exists()
                ).scalar()
            else:
                has_more = self.sesh.query(
                    table_clause.select().where(table_vals > keyset_vals).exists()
                ).scalar()
                has_less = False

        rows = [
            {c: row_tup._mapping[c.name] for c in table.columns}
            for row_tup in row_tuples
        ]

        return Page(
            has_less=has_less,
            has_more=has_more,
            rows=rows[: keyset.size],
        )

    def table_as_rows(
        self,
        table_uuid: UUID,
    ) -> Iterable[Sequence[PythonType]]:
        # To a first approximation this is about 10 times slower than COPY

        batchsize = 10_000
        table_clause = self._get_userdata_tableclause(table_uuid)
        columns = self.get_columns(table_uuid)
        q = (
            select(*[getattr(table_clause.c, c.name) for c in columns])
            .order_by(table_clause.c.csvbase_row_id)
            .execution_options(yield_per=batchsize)
        )
        yield from self.sesh.execute(q)

    def insert_table_data(
        self,
        table: Table,
        columns: Sequence[Column],
        rows: Iterable[Sequence[PythonType]],
    ) -> None:
        temp_table_name = self._make_temp_table_name(prefix="insert")
        main_table_name = self._make_userdata_table_name(
            table.table_uuid, with_schema=True
        )
        main_tableclause = self._get_userdata_tableclause(table.table_uuid)
        self.sesh.execute(
            CreateTempTableLike(satable(temp_table_name), main_tableclause)
        )

        raw_conn = self.sesh.connection().connection
        column_names = [c.name for c in columns]
        copy_manager = CopyManager(
            raw_conn,
            temp_table_name,
            column_names,
        )
        copy_manager.copy(rows)

        temp_tableclause = self._get_tableclause(temp_table_name, table.columns)

        add_stmt_select_columns = [getattr(temp_tableclause.c, c) for c in column_names]
        add_stmt_no_blanks = main_tableclause.insert().from_select(
            column_names,
            select(*add_stmt_select_columns)
            .select_from(temp_tableclause)
            .where(temp_tableclause.c.csvbase_row_id.is_not(None)),
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

        self.sesh.execute(add_stmt_no_blanks)
        self.sesh.execute(reset_serial_stmt)
        self.sesh.execute(add_stmt_blanks)

    def delete_table_data(self, table: Table) -> None:
        """Delete all data in the table.

        The csvbase_row_id sequence is not reset in this case to avoid confusion.

        """
        main_tableclause = self._get_userdata_tableclause(table.table_uuid)
        main_fullname = main_tableclause.fullname
        # FIXME: should consider DELETE if table is small - that's faster in
        # that case
        truncate_stmt = text(f"TRUNCATE {main_fullname};")
        self.sesh.execute(truncate_stmt)

    def drop_table(self, table_uuid: UUID) -> None:
        sa_table = self._get_userdata_tableclause(table_uuid)
        self.sesh.execute(DropTable(sa_table))  # type: ignore

    def create_table(self, table_uuid: UUID, columns: Iterable[Column]) -> UUID:
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
            self._make_userdata_table_name(table_uuid),
            MetaData(),
            *cols,
            schema="userdata",
        )
        self.sesh.execute(CreateTable(table))
        return table_uuid

    def copy_table_data(self, from_table_uuid: UUID, to_table_uuid: UUID) -> None:
        from_tableclause = self._get_userdata_tableclause(from_table_uuid)
        to_tableclause = self._get_userdata_tableclause(to_table_uuid)
        stmt = to_tableclause.insert().from_select(
            list(from_tableclause.c), from_tableclause
        )
        self.sesh.execute(stmt)
        self._reset_pk_sequence(to_tableclause)

    # FIXME: this is "replace", not "upsert"
    def upsert_table_data(
        self,
        table: Table,
        row_columns: Sequence[Column],
        rows: Iterable[Sequence[PythonType]],
        key: Sequence[Column] = (ROW_ID_COLUMN,),
    ) -> None:
        """Upsert table data from rows into the SQL table.

        Note that the columns being upserted can be a subset of the columns
        that are present in the SQL table.  If the csvbase_row_id column is
        present, it will be used to correlate changes.

        """

        # First, make a temp table and COPY the new rows into it
        temp_table_name = self._make_temp_table_name(prefix="upsert")
        main_table_name = self._make_userdata_table_name(
            table.table_uuid, with_schema=True
        )
        main_tableclause = self._get_userdata_tableclause(table.table_uuid)
        self.sesh.execute(
            CreateTempTableLike(satable(temp_table_name), main_tableclause)
        )
        raw_conn = self.sesh.connection().connection
        upsert_column_names = [c.name for c in row_columns]
        existing_column_names = [c.name for c in table.columns]
        copy_manager = CopyManager(raw_conn, temp_table_name, upsert_column_names)
        copy_manager.copy(rows)

        # Next selectively use the temp table to update the 'main' one
        temp_tableclause = self._get_tableclause(temp_table_name, table.columns)

        join_clause = [
            (main_tableclause.c[key_column.name] == temp_tableclause.c[key_column.name])
            for key_column in key
        ]

        # 1. for removals
        ids_to_delete = (
            select(main_tableclause.c.csvbase_row_id)
            .select_from(
                main_tableclause.outerjoin(
                    temp_tableclause,
                    and_(*join_clause),
                )
            )
            .where(temp_tableclause.c[key[0].name].is_(None))
        )
        remove_stmt = delete(main_tableclause).where(
            main_tableclause.c.csvbase_row_id.in_(ids_to_delete)
        )

        # 2. updates
        update_values = {}
        for col in table.columns:
            if col == ROW_ID_COLUMN and ROW_ID_COLUMN not in key:
                update_values[col.name] = func.coalesce(
                    main_tableclause.c.csvbase_row_id,
                    func.nextval(
                        func.pg_get_serial_sequence(main_table_name, "csvbase_row_id")
                    ),
                )
            else:
                update_values[col.name] = getattr(temp_tableclause.c, col.name)
        update_stmt = (
            main_tableclause.update().values(**update_values).where(and_(*join_clause))
        )

        # 3a. and additions where the csvbase_row_id as been set
        add_stmt_select_columns = [
            getattr(temp_tableclause.c, c.name) for c in table.columns
        ]
        add_stmt_no_blanks = main_tableclause.insert().from_select(
            existing_column_names,
            select(*add_stmt_select_columns)
            .select_from(
                temp_tableclause.outerjoin(
                    main_tableclause,
                    main_tableclause.c.csvbase_row_id
                    == temp_tableclause.c.csvbase_row_id,
                )
            )
            .where(
                main_tableclause.c.csvbase_row_id.is_(None),
                temp_tableclause.c.csvbase_row_id.is_not(None),
            ),
        )

        # 3b. reset the sequence that allocates pks
        # <done by _reset_pk_sequence>

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
            select(*select_columns)
            .select_from(
                temp_tableclause.outerjoin(main_tableclause, and_(*join_clause))
            )
            .where(
                main_tableclause.c.csvbase_row_id.is_(None),
                temp_tableclause.c.csvbase_row_id.is_(None),
            ),
        )

        self.sesh.execute(remove_stmt)
        self.sesh.execute(update_stmt)
        self.sesh.execute(add_stmt_no_blanks)
        self._reset_pk_sequence(main_tableclause)
        self.sesh.execute(add_stmt_blanks)

    def byte_count(self, table_uuid: UUID) -> int:
        # pg_total_relation_size returns the size of the table plus toast, plus
        # indices https://stackoverflow.com/a/70397779
        stmt = text("SELECT pg_total_relation_size(:relname);")
        rs = self.sesh.execute(
            stmt,
            params={
                "relname": self._make_userdata_table_name(table_uuid, with_schema=True)
            },
        )
        return cast(int, rs.scalar())


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
    # count and possibly include an exact count as well.
    stmt = """
SELECT
    CASE WHEN reltuples >= 1000 THEN
        null
    ELSE
        (
            SELECT
                COUNT(*)
            FROM
                %s)
    END,
    reltuples::bigint
FROM
    pg_class
WHERE
    relname = '%s';
    """
    x = stmt % (compiler.process(element.table, asfrom=True, **kw), element.table.name)
    return x
