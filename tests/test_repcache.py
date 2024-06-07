from datetime import datetime, timezone

from csvbase.value_objs import ContentType
from csvbase.repcache import RepCache

from .utils import random_uuid, random_df


def test_repcache__miss():
    repcache = RepCache()

    assert not repcache.exists(
        random_uuid(), ContentType.CSV, datetime.now(timezone.utc)
    )


def test_repcache__hit():
    repcache = RepCache()

    table_uuid = random_uuid()
    content_type = ContentType.CSV
    last_changed = datetime.now(timezone.utc)

    contents = b"a,b,c\n1,2,3"

    with repcache.open(table_uuid, content_type, last_changed, "wb") as rep_file:
        rep_file.write(contents)

    assert repcache.exists(table_uuid, content_type, last_changed)

    with repcache.open(table_uuid, content_type, last_changed, "rb") as rep_file:
        assert rep_file.read() == contents


def test_repcache__update_wipes_out_old_reps():
    repcache = RepCache()

    table_uuid = random_uuid()
    content_type = ContentType.CSV

    initial_dt = datetime(2018, 1, 3, tzinfo=timezone.utc)
    initial_contents = b"a,b,c\n1,2,3"

    with repcache.open(table_uuid, content_type, initial_dt, "wb") as rep_file:
        rep_file.write(initial_contents)

    assert repcache.exists(table_uuid, content_type, initial_dt)

    update_dt = datetime(2018, 1, 4, tzinfo=timezone.utc)
    update_contents = b"a,b,c\n4,5,6"

    with repcache.open(table_uuid, content_type, update_dt, "wb") as rep_file:
        rep_file.write(update_contents)

    assert not repcache.exists(table_uuid, content_type, initial_dt)
    assert repcache.exists(table_uuid, content_type, update_dt)


def test_repcache__sizes():
    repcache = RepCache()

    table_uuid = random_uuid()
    last_changed = datetime.now(timezone.utc)
    df = random_df()

    with repcache.open(table_uuid, ContentType.CSV, last_changed, "wb") as rep_file:
        df.to_csv(rep_file)

    with repcache.open(table_uuid, ContentType.PARQUET, last_changed, "wb") as rep_file:
        df.to_parquet(rep_file)

    sizes = repcache.sizes(table_uuid, last_changed)
    assert {ContentType.CSV, ContentType.PARQUET} == set(sizes.keys())
    assert {int} == set(type(v) for v in sizes.values())


def test_repcache__path():
    repcache = RepCache()

    table_uuid = random_uuid()
    last_changed = datetime(2018, 1, 3, tzinfo=timezone.utc)
    df = random_df()

    with repcache.open(table_uuid, ContentType.CSV, last_changed, "wb") as rep_file:
        df.to_csv(rep_file)

    expected = f"{table_uuid}/2018-01-03T00_00_00+00_00.csv"
    actual = repcache.path(table_uuid, ContentType.CSV, last_changed)
    assert expected == actual
