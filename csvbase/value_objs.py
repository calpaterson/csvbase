from typing import Optional, Sequence, Tuple
from typing_extensions import Literal
from uuid import UUID
from datetime import datetime
from dataclasses import dataclass


@dataclass
class User:
    user_uuid: UUID
    username: str
    email: Optional[str]
    registered: datetime


@dataclass
class KeySet:
    n: int
    op: Literal["greater_than", "less_than"]
    size: int = 10


@dataclass
class Page:
    has_less: bool
    has_more: bool
    rows: Sequence[Tuple]
