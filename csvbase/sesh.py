from typing import cast

from sqlalchemy.orm import Session

from .db import db


def get_sesh() -> Session:
    # This isn't actually a session, but it has almost the same interface.
    # There may be a better way to type hint this
    return cast(Session, db.session)
