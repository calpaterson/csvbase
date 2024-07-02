from datetime import datetime, timezone

from csvbase.value_objs import ContentType
from csvbase.repcache import RepCache

from .utils import random_uuid, random_df


def test_repcache__miss():
    repcache = RepCache(random_uuid(), ContentType.CSV, datetime.now(timezone.utc))

    assert not repcache.exists()


def test_repcache__hit():
    table_uuid = random_uuid()
    content_type = ContentType.CSV
    last_changed = datetime.now(timezone.utc)

    repcache = RepCache(table_uuid, content_type, last_changed)

    contents = b"a,b,c\n1,2,3"

    with repcache.open("wb") as rep_file:
        rep_file.write(contents)

    assert repcache.exists()

    with repcache.open("rb") as rep_file:
        assert rep_file.read() == contents


def test_repcache__update_wipes_out_old_reps():
    table_uuid = random_uuid()
    content_type = ContentType.CSV

    initial_dt = datetime(2018, 1, 3, tzinfo=timezone.utc)
    initial_repcache = RepCache(table_uuid, content_type, initial_dt)
    initial_contents = b"a,b,c\n1,2,3"

    with initial_repcache.open("wb") as rep_file:
        rep_file.write(initial_contents)

    assert initial_repcache.exists()

    update_dt = datetime(2018, 1, 4, tzinfo=timezone.utc)
    update_contents = b"a,b,c\n4,5,6"
    update_repcache = RepCache(table_uuid, content_type, update_dt)

    with update_repcache.open("wb") as rep_file:
        rep_file.write(update_contents)

    assert not initial_repcache.exists()
    assert update_repcache.exists()


def test_repcache__sizes():
    table_uuid = random_uuid()
    last_changed = datetime.now(timezone.utc)
    df = random_df()

    csv_repcache = RepCache(table_uuid, ContentType.CSV, last_changed)
    with csv_repcache.open("wb") as rep_file:
        df.to_csv(rep_file)

    parquet_repcache = RepCache(table_uuid, ContentType.PARQUET, last_changed)
    with parquet_repcache.open("wb") as rep_file:
        df.to_parquet(rep_file)

    sizes = RepCache.sizes(table_uuid, last_changed)
    assert {ContentType.CSV, ContentType.PARQUET} == set(sizes.keys())
    assert {int} == set(type(v) for v in sizes.values())


def test_repcache__path():
    table_uuid = random_uuid()
    last_changed = datetime(2018, 1, 3, tzinfo=timezone.utc)
    repcache = RepCache(table_uuid, ContentType.CSV, last_changed)

    df = random_df()

    with repcache.open("wb") as rep_file:
        df.to_csv(rep_file)

    expected = f"{table_uuid}/2018-01-03T00_00_00+00_00.csv"
    actual = repcache.path()
    assert expected == actual
