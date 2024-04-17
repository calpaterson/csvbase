from typing import Protocol, Iterable, List, Sequence, Tuple, Optional
from uuid import UUID

from ..value_objs import Column, Table, PythonType, KeySet, Page


# FIXME: this is a work in progress
class UserdataAdapter(Protocol):
    def create_table(self, table_uuid: UUID, columns: Iterable[Column]): ...

    def insert_table_data(
        self,
        table: Table,
        columns: Sequence[Column],
        rows: Iterable[Sequence[PythonType]],
    ): ...

    def table_page(self, table: Table, keyset: KeySet) -> Page: ...

    def row_id_bounds(
        self, table_uuid: UUID
    ) -> Tuple[Optional[int], Optional[int]]: ...

    def get_columns(self, table_uuid: UUID) -> List[Column]: ...

    def table_as_rows(self, table_uuid: UUID) -> Iterable[Sequence[PythonType]]: ...
