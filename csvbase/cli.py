import sys
from pathlib import Path
from typing import Optional
from logging import getLogger

import click
from sqlalchemy.sql.expression import text

from .value_objs import DataLicence, ContentType
from .models import Base
from .logging import configure_logging
from .config import load_config, default_config_file
from csvbase import svc, table_io
from .sesh import get_sesh
from .web.app import init_app
from .web.billing import svc as billing_svc
from .follow.git import GitSource
from .follow.update import update_external_table
from .userdata import PGUserdataAdapter
from .repcache import RepCache

logger = getLogger(__name__)


@click.command(help="Load the prohibited username list into the database")
def load_prohibited_usernames():
    with init_app().app_context():
        svc.load_prohibited_usernames(get_sesh())


@click.command(
    help="Make the tables in the database (from the models, without using alembic)"
)
def make_tables():
    with init_app().app_context():
        sesh = get_sesh()
        sesh.execute("CREATE SCHEMA IF NOT EXISTS metadata")
        sesh.execute("CREATE SCHEMA IF NOT EXISTS userdata")
        Base.metadata.create_all(bind=sesh.connection(), checkfirst=True)

        dl_insert = text(
            """
        INSERT INTO metadata.data_licences (licence_id, licence_name)
            VALUES (:licence_id, :licence_name)
        ON CONFLICT
            DO NOTHING
        """
        )
        with sesh.begin() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")
            conn.execute(
                dl_insert,
                [{"licence_id": e.value, "licence_name": e.name} for e in DataLicence],
            )

        alembic_version_ddl = """
        CREATE TABLE IF NOT EXISTS metadata.alembic_version (
            version_num varchar(32) NOT NULL);
        ALTER TABLE metadata.alembic_version DROP CONSTRAINT IF EXISTS alembic_version_pkc;
        ALTER TABLE ONLY metadata.alembic_version
            ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);
        """

        with sesh.begin() as conn:
            conn.execute(alembic_version_ddl)
            conn.execute(
                """
        INSERT INTO alembic_version (version_num)
            VALUES ('created by make_tables')
        ON CONFLICT
           DO NOTHING;
        """
            )


@click.command("csvbase-config")
@click.option(
    "-f",
    "--config-file",
    default=None,
    help="Path to config file",
    type=click.Path(exists=True, dir_okay=False, path_type=Path),
)
def config_cli(config_file: Optional[Path]):
    configure_logging()

    if config_file is None:
        config_file = default_config_file()

    logger.info(load_config(config_file))


@click.command("csvbase-update-stripe-subscriptions")
@click.option(
    "--full",
    is_flag=True,
    default=False,
    help="Update all subscriptions, not just those that haven't been updated recently",
)
def update_stripe_subscriptions(full: bool) -> None:
    configure_logging()

    sesh = get_sesh()

    billing_svc.initialise_stripe()
    app = init_app()
    with app.app_context():
        all_updated = billing_svc.update_stripe_subscriptions(sesh, full)
        if not all_updated:
            sys.exit(1)


@click.command("csvbase-update-external-tables")
def update_external_tables() -> None:
    configure_logging()
    sesh = get_sesh()
    app = init_app()
    error_count = 0
    with app.app_context():
        git_source = GitSource()
        backend = PGUserdataAdapter(sesh)
        for table, source in svc.git_tables(sesh):
            logger.info("retrieving %s", source.repo_url)
            try:
                with git_source.retrieve(
                    source.repo_url, source.branch, source.path
                ) as upstream_file:
                    if upstream_file.version != source.version():
                        update_external_table(sesh, backend, table, upstream_file)
                        svc.mark_table_changed(sesh, table.table_uuid)
                        sesh.commit()
            except Exception:
                error_count += 1
                logger.exception(
                    "exception while updating %s/%s", table.username, table.table_name
                )
                sesh.rollback()
    sys.exit(0 if error_count == 0 else 1)


@click.command("csvbase-repcache-populate")
@click.argument("ref")
def repcache_populate(ref: str) -> None:
    configure_logging()
    sesh = get_sesh()
    repcache = RepCache()
    app = init_app()
    username, table_name = ref.split("/")
    with app.app_context():
        table = svc.get_table(sesh, username, table_name)
        backend = PGUserdataAdapter(sesh)
        for content_type in [
            ContentType.CSV,
            ContentType.PARQUET,
            ContentType.JSON_LINES,
        ]:
            exists = repcache.exists(table.table_uuid, content_type, table.last_changed)
            if exists:
                logger.info("not regenerating %s - already up-to-date", content_type)
            else:
                # FIXME: this needs to be centralised somewhere, instead of copy-pasted
                with repcache.open(
                    table.table_uuid, content_type, table.last_changed, mode="wb"
                ) as rep_file:
                    columns = backend.get_columns(table.table_uuid)
                    rows = backend.table_as_rows(table.table_uuid)
                    if content_type is ContentType.PARQUET:
                        table_io.rows_to_parquet(columns, rows, rep_file)
                    elif content_type is ContentType.JSON_LINES:
                        table_io.rows_to_jsonlines(columns, rows, rep_file)
                    elif content_type is ContentType.XLSX:
                        table_io.rows_to_xlsx(
                            columns, rows, excel_table=False, buf=rep_file
                        )
                    else:
                        table_io.rows_to_csv(columns, rows, buf=rep_file)
