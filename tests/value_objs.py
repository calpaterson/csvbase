from dataclasses import dataclass

from csvbase.value_objs import User


@dataclass
class ExtendedUser(User):
    password: str
