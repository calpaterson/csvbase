from flask_sqlalchemy import SQLAlchemy

from csvbase.config import get_config

db = SQLAlchemy()


def get_db_url() -> str:
    return get_config().db_url
