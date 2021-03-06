from datetime import date

import pytest

from csvbase import exc
from csvbase.conv import DateConverter, IntegerConverter


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param(["2018-01-03"], True, id="iso"),
        pytest.param([" 2018-01-03"], True, id="with whitespace (1)"),
        pytest.param(["2018-01-03 "], True, id="with whitespace (2)"),
        pytest.param([" 2018-01-03 "], True, id="with whitespace (3)"),
        pytest.param(["2018-01-03T00:00:00"], False, id="datetime without zone"),
        pytest.param(["2018-01-03", ""], True, id="one missing value"),
        pytest.param([""], False, id="all missing values"),
    ],
)
def test_DateConverter__sniff(inp, expected):
    dc = DateConverter()
    assert dc.sniff(inp) is expected


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param("2018-01-03", date(2018, 1, 3), id="iso"),
        pytest.param(" 2018-01-03", date(2018, 1, 3), id="with whitespace (1)"),
        pytest.param("2018-01-03 ", date(2018, 1, 3), id="with whitespace (2)"),
        pytest.param(" 2018-01-03 ", date(2018, 1, 3), id="with whitespace (3)"),
        pytest.param("", None, id="whitespace (1)"),
        pytest.param(" ", None, id="whitespace (2)"),
        pytest.param("  ", None, id="whitespace (3)"),
    ],
)
def test_DateConverter__convert(inp, expected):
    dc = DateConverter()
    assert dc.convert(inp) == expected


def test_DateConverter__convert_failure():
    dc = DateConverter()
    with pytest.raises(exc.UnconvertableValueException):
        dc.convert("nonsense")


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param(["1"], True, id="an int"),
        pytest.param(["-1"], True, id="a negative int"),
        pytest.param(["1.0"], False, id="a float"),
        pytest.param(["1000"], True, id="thousand"),
        pytest.param(["1,000"], True, id="financial thousand"),
        pytest.param([" 1,000 "], True, id="whitespace"),
        pytest.param(["-1,000 "], True, id="negative financial"),
    ],
)
def test_IntegerConverter__sniff(inp, expected):
    ic = IntegerConverter()
    assert ic.sniff(inp) is expected


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param("1", 1, id="an int"),
        pytest.param("-1", -1, id="a negative int"),
        pytest.param("1000", 1000, id="thousand"),
        pytest.param("1,000", 1000, id="financial thousand"),
        pytest.param(" 1,000 ", 1000, id="whitespace"),
        pytest.param("-1,000 ", -1000, id="negative financial"),
        pytest.param(" ", None, id="whitespace"),
        pytest.param("", None, id="blank"),
    ],
)
def test_IntegerConverter__convert(inp, expected):
    ic = IntegerConverter()
    assert ic.convert(inp) == expected


def test_IntegerConverter__convert_failure():
    ic = IntegerConverter()
    with pytest.raises(exc.UnconvertableValueException):
        ic.convert("nonsense")
