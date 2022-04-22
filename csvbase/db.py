from os import environ

from sqlalchemy import create_engine
from sqlalchemy.sql.expression import text

from .value_objs import DataLicence
from .models import Base

if "HEROKU" in environ:
    # we're running on heroku
    engine = create_engine(
        environ["DATABASE_URL"].replace("postgres://", "postgresql://")
    )
else:
    engine = create_engine(environ.get("CSVBASE_DB_URL", "postgresql:///csvbase"))


def make_tables():
    with engine.begin() as conn:
        conn.execute("CREATE SCHEMA IF NOT EXISTS metadata")
        conn.execute("CREATE SCHEMA IF NOT EXISTS userdata")
    Base.metadata.create_all(bind=engine, checkfirst=True)

    dl_insert = text(
        """
    INSERT INTO metadata.data_licences (licence_id, licence_name)
        VALUES (:licence_id, :licence_name)
    ON CONFLICT
        DO NOTHING
    """
    )
    with engine.begin() as conn:
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

    with engine.begin() as conn:
        conn.execute(alembic_version_ddl)
        conn.execute(
            """
    INSERT INTO alembic_version (version_num)
        VALUES ('created by make_tables')
    ON CONFLICT
       DO NOTHING;
    """
        )
