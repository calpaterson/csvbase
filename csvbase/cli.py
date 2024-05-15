import sys
from pathlib import Path
from typing import Optional
from logging import getLogger

import click
from sqlalchemy.sql.expression import text

from .value_objs import DataLicence
from .models import Base
from .logging import configure_logging
from .config import load_config, default_config_file
from csvbase import svc
from .sesh import get_sesh
from .web.app import init_app
from .web.billing import svc as billing_svc
from .follow.git import GitSource
from .follow.update import update_external_table
from .userdata import PGUserdataAdapter

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
                        update_external_table(
                            sesh, backend, table, upstream_file, source
                        )
                        svc.mark_table_changed(sesh, table.table_uuid)
                        sesh.commit()
            except Exception:
                error_count += 1
                logger.exception(
                    "exception while updating %s/%s", table.username, table.table_name
                )
                sesh.rollback()
    sys.exit(0 if error_count == 0 else 1)
