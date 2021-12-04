from os import environ

from sqlalchemy import create_engine

from .models import Base

engine = create_engine(environ.get("CSVBASE_DB_URL", "postgresql:///csvbase"))

def make_tables():
    Base.metadata.create_all(bind=engine, checkfirst=True)
