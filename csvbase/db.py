from os import environ

from sqlalchemy import create_engine

engine = create_engine(environ.get("CSVBASE_DB_URL", "postgresql:///csvbase"))
