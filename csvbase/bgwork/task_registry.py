from uuid import UUID
from typing import cast
from datetime import timedelta
from urllib.parse import urlparse
from logging import getLogger

from celery import Celery

from csvbase.web.billing import svc as billing_svc
from csvbase.value_objs import GitUpstream
from csvbase.userdata import PGUserdataAdapter
from csvbase.sesh import get_sesh
from csvbase import svc
from csvbase.bgwork.core import celery
from csvbase.follow import update
from csvbase.follow.git import GitSource

logger = getLogger(__name__)


def is_test_url(url: str) -> bool:
    """The tests will put git url in the database as "example.com" - this helps
    exclude them when running locally.

    """
    parsed = urlparse(url)
    return parsed.netloc.endswith("example.com")


@celery.task
def update_external_tables() -> None:
    sesh = get_sesh()
    for table, source in svc.git_tables(sesh):
        if not is_test_url(source.repo_url):
            update_external_table.delay(table.table_uuid)


@celery.task
def update_external_table(table_uuid: UUID) -> None:
    sesh = get_sesh()
    git_source = GitSource()
    backend = PGUserdataAdapter(sesh)
    table = svc.get_table_by_uuid(sesh, table_uuid)
    source = cast(GitUpstream, table.upstream)
    with git_source.retrieve(
        source.repo_url, source.branch, source.path
    ) as upstream_file:
        if upstream_file.version != source.version():
            update.update_external_table(sesh, backend, table, upstream_file)
            svc.mark_table_changed(sesh, table.table_uuid)
            sesh.commit()

@celery.task
def update_stripe_subscriptions() -> None:
    sesh = get_sesh()
    billing_svc.initialise_stripe()
    billing_svc.update_stripe_subscriptions(sesh, full=False)


@celery.on_after_configure.connect
def setup_periodic_tasks(sender: Celery, **kwargs) -> None:
    """Sets up the various periodic tasks for celery beat."""
    sender.add_periodic_task(
        timedelta(minutes=30).total_seconds(), update_external_tables.s()
    )
    sender.add_periodic_task(
        timedelta(days=1).total_seconds(), update_stripe_subscriptions.s()
    )
