from os import environ

from sqlalchemy import create_engine

from .models import Base

if "HEROKU" in environ:
    # we're running on heroku
    engine = create_engine(environ["DATABASE_URL"].replace("postgres://", "postgresql://"))
else:
    engine = create_engine(environ.get("CSVBASE_DB_URL", "postgresql:///csvbase"))


def make_tables():
    Base.metadata.create_all(bind=engine, checkfirst=True)
