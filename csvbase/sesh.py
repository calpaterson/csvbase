from sqlalchemy.orm import Session
from flask import current_app


def get_sesh() -> Session:
    return current_app.scoped_session  # type: ignore
