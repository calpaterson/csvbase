from flask_sqlalchemy import SQLAlchemy

from csvbase.config import get_config

db = SQLAlchemy(engine_options={"future": True}, session_options={"future": True})


def get_db_url() -> str:
    return get_config().db_url
