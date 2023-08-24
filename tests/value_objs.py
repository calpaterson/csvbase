from base64 import b64encode
from dataclasses import dataclass

from csvbase.value_objs import User


@dataclass
class ExtendedUser(User):
    password: str

    def basic_auth(self) -> str:
        """The HTTP Basic Auth header value for this user"""
        hex_api_key = self.hex_api_key()
        user_pass = f"{self.username}:{hex_api_key}".encode("utf-8")
        encoded = b64encode(user_pass).decode("utf-8")
        return f"Basic {encoded}"
