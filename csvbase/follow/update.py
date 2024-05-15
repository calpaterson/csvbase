from typing import Sequence
from logging import getLogger

from sqlalchemy.orm import Session

from ..value_objs import (
    ROW_ID_COLUMN,
    Column,
    Table,
    UpstreamFile,
)
from .. import svc, streams, table_io
from ..userdata import PGUserdataAdapter

logger = getLogger(__name__)


def update_external_table(
    sesh: Session,
    backend: PGUserdataAdapter,
    table: Table,
    upstream_file: UpstreamFile,
) -> None:
    logger.info("updating %s/%s", table.username, table.table_name)
    str_buf = streams.byte_buf_to_str_buf(upstream_file.filelike)
    dialect, csv_columns = streams.peek_csv(str_buf, table.columns)
    rows = table_io.csv_to_rows(str_buf, csv_columns, dialect)
    key_column_names = svc.get_key(sesh, table.table_uuid)
    key: Sequence[Column]
    if len(key_column_names) > 0:
        key = [c for c in table.user_columns() if c.name in key_column_names]
    else:
        key = (ROW_ID_COLUMN,)
    backend.upsert_table_data(table, csv_columns, rows, key=key)
    svc.set_version(sesh, table.table_uuid, upstream_file.version)
