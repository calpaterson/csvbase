import sys
from pathlib import Path
from typing import Optional
from logging import getLogger

import click
from sqlalchemy.sql.expression import text
from sqlalchemy.dialects.postgresql import insert as pginsert

from .value_objs import DataLicence, ContentType, LICENCE_MAP
from . import models
from .logging import configure_logging
from .config import load_config, default_config_file
from csvbase import svc, comments_svc
from .sesh import get_sesh
from .web.app import init_app
from .web.billing import svc as billing_svc

logger = getLogger(__name__)


@click.command(help="Load the prohibited username list into the database")
def load_prohibited_usernames():
    with init_app().app_context():
        svc.load_prohibited_usernames(get_sesh())


@click.command(
    help="Make the tables in the database (from the models, without using alembic)"
)
def make_tables() -> None:
    with init_app().app_context():
        sesh = get_sesh()
        sesh.execute(text("CREATE SCHEMA IF NOT EXISTS metadata"))
        sesh.execute(text("CREATE SCHEMA IF NOT EXISTS userdata"))
        models.Base.metadata.create_all(bind=sesh.connection(), checkfirst=True)

        dl_insert = text(
            """
        INSERT INTO metadata.data_licences (licence_id, licence_name)
            VALUES (:licence_id, :licence_name)
        ON CONFLICT
            DO NOTHING
        """
        )
        with sesh.begin():
            sesh.execute(text("CREATE SCHEMA IF NOT EXISTS metadata"))
            sesh.execute(
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

        with sesh.begin():
            sesh.execute(text(alembic_version_ddl))
            sesh.execute(
                text(
                    """
        INSERT INTO alembic_version (version_num)
            VALUES ('created by make_tables')
        ON CONFLICT
           DO NOTHING;
        """
                )
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


@click.command("csvbase-repcache-populate")
@click.argument("ref")
def repcache_populate(ref: str) -> None:
    configure_logging()
    sesh = get_sesh()
    app = init_app()
    username, table_name = ref.split("/")
    with app.app_context():
        table = svc.get_table(sesh, username, table_name)
        for content_type in [
            ContentType.CSV,
            ContentType.PARQUET,
            ContentType.JSON_LINES,
        ]:
            svc.populate_repcache(sesh, table.table_uuid, content_type)


@click.command("csvbase-create-thread")
@click.argument("creator")
@click.argument("title")
def create_thread(creator: str, title: str) -> None:
    sesh = get_sesh()
    app = init_app()
    with app.app_context():
        creator_user = svc.user_by_name(sesh, creator)
        thread = comments_svc.create_thread(sesh, creator_user, title)
        sesh.commit()
    logger.info("Created thread: '%s'", thread.slug)


@click.command("populate-licences")
def populate_licences() -> None:
    sesh = get_sesh()
    app = init_app()
    with app.app_context():
        insert_stmt = pginsert(models.Licence)
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=["spdx_id"],
            set_=dict(licence_name=insert_stmt.excluded.licence_name),
        )
        sesh.execute(
            upsert_stmt,
            [
                {"spdx_id": licence.spdx_id, "licence_name": licence.name}
                for licence in LICENCE_MAP.values()
            ],
        )
        sesh.commit()
    logger.info("populated licences")
