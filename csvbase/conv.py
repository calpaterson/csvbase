import re
from datetime import datetime, date
from typing import Iterable, Optional

WHITESPACE_REGEX = re.compile(r"^ *$")


class DateConverter:
    DATE_REGEX = re.compile(r"^ ?\d{4}-\d{2}-\d{2} ?$")
    DATE_FORMAT = "%Y-%m-%d"

    def sniff(self, values: Iterable[str]) -> bool:
        non_match = False
        one_match = False
        for value in values:
            if self.DATE_REGEX.match(value):
                one_match = True
            elif WHITESPACE_REGEX.match(value):
                continue
            else:
                non_match = True
                break
        return (non_match is False) and one_match

    def convert(self, value: str) -> Optional[date]:
        stripped = value.strip()
        if stripped == "":
            return None
        else:
            return date.fromisoformat(stripped)
