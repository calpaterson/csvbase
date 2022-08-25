import re
from datetime import date
from typing import Iterable, Optional, Pattern

from . import exc, conv
from .value_objs import ColumnType, PythonType

WHITESPACE_REGEX = re.compile(r"^ *$")


def sniff_and_allow_blanks(regex: Pattern, values: Iterable[str]) -> bool:
    """This function takes a regex and looks at the values, return if:
    - at least one value matches the regex
    - the others are blanks

    and false otherwise."""
    non_match = False
    one_match = False
    for value in values:
        if regex.match(value):
            one_match = True
        elif WHITESPACE_REGEX.match(value):
            continue
        else:
            non_match = True
            break
    return (non_match is False) and one_match


class DateConverter:
    DATE_REGEX = re.compile(r"^ ?\d{4}-\d{2}-\d{2} ?$")
    DATE_FORMAT = "%Y-%m-%d"

    def sniff(self, values: Iterable[str]) -> bool:
        return sniff_and_allow_blanks(self.DATE_REGEX, values)

    def convert(self, value: str) -> Optional[date]:
        stripped = value.strip()
        if stripped == "":
            return None

        try:
            return date.fromisoformat(stripped)
        except ValueError:
            raise exc.UnconvertableValueException(ColumnType.DATE, value)


class IntegerConverter:
    INTEGER_REGEX = re.compile(r"^ ?-?(\d|,| )+$")

    def sniff(self, values: Iterable[str]) -> bool:
        return sniff_and_allow_blanks(self.INTEGER_REGEX, values)

    def convert(self, value: str) -> Optional[int]:
        stripped = value.strip()
        if stripped == "":
            return None
        match = self.INTEGER_REGEX.match(value)
        if not match:
            raise exc.UnconvertableValueException(ColumnType.INTEGER, value)
        return int(match.group().replace(",", ""))


class FloatConverter:
    FLOAT_REGEX = re.compile(r"^ ?-?(\d|,|\.| )+$")

    def sniff(self, values: Iterable[str]) -> bool:
        return sniff_and_allow_blanks(self.FLOAT_REGEX, values)

    def convert(self, value: str) -> Optional[float]:
        stripped = value.strip()
        if stripped == "":
            return None
        match = self.FLOAT_REGEX.match(value)
        if not match:
            raise exc.UnconvertableValueException(ColumnType.FLOAT, value)
        return float(match.group().replace(",", ""))


class BooleanConverter:
    BOOLEAN_REGEX = re.compile(r"^ ?(TRUE|FALSE|T|F|YES|NO|Y|N) ?$", re.I)
    TRUE_REGEX = re.compile(r"^(TRUE|T|YES|Y)$", re.I)
    FALSE_REGEX = re.compile(r"^(FALSE|F|NO|N)$", re.I)

    def sniff(self, values: Iterable[str]) -> bool:
        return sniff_and_allow_blanks(self.BOOLEAN_REGEX, values)

    def convert(self, value: str) -> Optional[float]:
        stripped = value.strip()
        if stripped == "":
            return None

        false_match = self.FALSE_REGEX.match(stripped)
        if false_match:
            return False

        true_match = self.TRUE_REGEX.match(stripped)
        if true_match:
            return True

        raise exc.UnconvertableValueException(ColumnType.BOOLEAN, value)


def from_string_to_python(
    column_type: ColumnType, as_string: str
) -> Optional["PythonType"]:
    """Parses values from string (ie: csv) into Python objects, according
    to ColumnType."""
    if as_string == "" or as_string is None:
        return None
    if column_type is ColumnType.BOOLEAN:
        bc = conv.BooleanConverter()
        return bc.convert(as_string)
    elif column_type is ColumnType.DATE:
        dc = conv.DateConverter()
        return dc.convert(as_string)
    elif column_type is ColumnType.INTEGER:
        ic = conv.IntegerConverter()
        return ic.convert(as_string)
    elif column_type is ColumnType.FLOAT:
        fc = conv.FloatConverter()
        return fc.convert(as_string)
    else:
        return column_type.python_type()(as_string)
