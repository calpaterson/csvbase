from uuid import UUID
from typing import cast
from datetime import timedelta
from urllib.parse import urlparse
from logging import getLogger

from celery import Celery

from csvbase.value_objs import GitUpstream
from csvbase.userdata import PGUserdataAdapter
from csvbase.sesh import get_sesh
from csvbase import svc
from csvbase.bgwork.core import celery
from csvbase.follow import update
from csvbase.follow.git import GitSource

logger = getLogger(__name__)

def is_test_url(url: str) -> bool:
    breakpoint()
    return urlparse(url).netloc == "example.com"


@celery.task
def update_external_tables() -> None:
    sesh = get_sesh()
    for table, source in svc.git_tables(sesh):
        if not is_test_url(source.repo_url):
            logger.critical(source.repo_url)
            update_external_table.delay(table.table_uuid)


@celery.task
def update_external_table(table_uuid: UUID):
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


@celery.on_after_configure.connect
def setup_periodic_tasks(sender: Celery, **kwargs) -> None:
    sender.add_periodic_task(
        timedelta(seconds=1).total_seconds(), update_external_tables.s()
    )
