import io
from datetime import datetime, timezone
import csv
from logging import getLogger
from logging import getLogger

from . import models

logger = getLogger(__name__)

PYTHON_TO_SQL_TYPEMAP = {
    int: "BIGINT",
    str: "TEXT",
    datetime: "TIMESTAMP WITH TIMEZONE",
    float: "DOUBLE PRECISION",
    bool: "BOOLEAN",
}


def types_for_csv(csv_buf, dialect):
    reader = csv.reader(csv_buf, dialect=dialect)
    # FIXME: hardcoded
    return {
        "cow name": str,
        "cow id": int,
    }


def create_user(sesh, username, password):
    sesh.add(
        models.User(
            uuid4(),
            username,
            "just junk for now",
            "Europe/London",
            datetime.now(timezone.utc),
        )
    )


def user_uuid_for_name(sesh, username):
    return (
        sesh.query(models.User.user_uuid)
        .filter(models.User.username == username)
        .scalar()
    )


def make_create_table_ddl(username, table_name, types):
    ddl = [f"CREATE TABLE {username}__{table_name} ("]
    for index, (column_name, column_type) in enumerate(types.items()):
        ddl.append('"')
        ddl.append(column_name)
        ddl.append('" ')
        ddl.append(PYTHON_TO_SQL_TYPEMAP[column_type])
        if (index + 1) != len(types):
            ddl.append(",")
    ddl.append(");")
    joined_ddl = "".join(ddl)
    logger.info("joined_ddl: %s", joined_ddl)
    return joined_ddl


def table_exists(sesh, user_uuid, table_name):
    return sesh.query(
        sesh.query(models.Table)
        .filter(
            models.Table.user_uuid == user_uuid, models.Table.table_name == table_name
        )
        .exists()
    ).scalar()


def get_columns(sesh, username, table_name):
    # lifted from https://dba.stackexchange.com/a/22420/28877
    stmt = f"""
    SELECT attname AS column_name, atttypid::regtype AS sql_type
    FROM   pg_attribute
    WHERE  attrelid = 'public.{username}__{table_name}'::regclass
    AND    attnum > 0
    AND    NOT attisdropped
    ORDER  BY attnum;
    """
    rs = sesh.execute(stmt)
    return [r[0] for r in rs]


def make_drop_table_ddl(username, table_name):
    return f"DROP TABLE {username}__{table_name};"


def upsert_table(sesh, user_uuid, username, table_name, csv_buf):
    dialect = csv.Sniffer().sniff(csv_buf.read(1024))
    logger.info("sniffed dialect: %s", dialect)
    csv_buf.seek(0)
    types = types_for_csv(csv_buf, dialect)
    csv_buf.seek(0)

    already_exists = table_exists(sesh, user_uuid, table_name)
    if already_exists:
        # FIXME: could truncate or delete all here to save time
        sesh.execute(make_drop_table_ddl(username, table_name))
        logger.info("dropped %s/%s", username, table_name)
    else:
        sesh.add(models.Table(user_uuid=user_uuid, table_name=table_name, public=True))

    sesh.execute(make_create_table_ddl(username, table_name, types))
    logger.info(
        "%s %s/%s", "(re)created" if already_exists else "created", username, table_name
    )

    cursor = sesh.connection().connection.cursor()
    csv_buf.readline()  # pop the header, with is not expected by copy_from

    # FIXME: error handling, sometimes people curl stuff with just --data
    # instead of --data-binary, which eats the newlines
    cursor.copy_from(csv_buf, f"{username}__{table_name}", sep=dialect.delimiter)


def get_table(sesh, user_uuid, username, table_name):
    csv_buf = io.StringIO()

    columns = get_columns(sesh, username, table_name)

    # this allows for putting the columns in with proper csv escaping
    header_writer = csv.writer(csv_buf)
    header_writer.writerow(columns)

    cursor = sesh.connection().connection.cursor()
    cursor.copy_to(csv_buf, f"{username}__{table_name}", sep=",", columns=columns)
    csv_buf.seek(0)
    return csv_buf


def is_public(sesh, username, table_name):
    return (
        sesh.query(models.Table.public)
        .join(models.User)
        .filter(models.User.username == username, models.Table.table_name == table_name)
        .scalar()
    )
