from typing import Optional
from uuid import UUID
from datetime import datetime
from dataclasses import dataclass

@dataclass
class User:
    user_uuid: UUID
    username: str
    email: Optional[str]
    registered: datetime
