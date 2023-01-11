from os import environ

from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def get_db_url() -> str:
    return environ.get("CSVBASE_DB_URL", "postgresql:///csvbase")
