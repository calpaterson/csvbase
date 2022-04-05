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
    Base.metadata.create_all(bind=engine, checkfirst=True)

    dl_insert = text(
        """
    INSERT INTO data_licences (licence_id, licence_name)
        VALUES (:licence_id, :licence_name)
    ON CONFLICT
        DO NOTHING
    """
    )
    with engine.begin() as conn:
        conn.execute(
            dl_insert,
            [{"licence_id": e.value, "licence_name": e.name} for e in DataLicence],
        )
