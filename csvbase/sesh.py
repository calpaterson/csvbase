from sqlalchemy.orm import Session

from .db import db


def get_sesh() -> Session:
    return db.session
