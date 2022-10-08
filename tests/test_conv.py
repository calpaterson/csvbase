from datetime import date

import pytest

from csvbase import exc
from csvbase.conv import (
    DateConverter,
    IntegerConverter,
    FloatConverter,
    BooleanConverter,
)


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
        pytest.param(["1000"], True, id="thousand"),
        pytest.param(["1,000"], True, id="financial thousand"),
        pytest.param([" 1,000 "], True, id="whitespace"),
        pytest.param(["-1,000 "], True, id="negative financial"),
        pytest.param(["1.0"], False, id="float not ok when sniffing"),
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
        pytest.param("1.0", 1, id="float ok when converting"),
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


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param(["1.0"], True, id="an float"),
        pytest.param(["-1.0"], True, id="a negative int"),
        pytest.param(["1."], True, id="a float (no trailing 0)"),
        pytest.param(["1"], True, id="an int (works ok)"),
        pytest.param(["1000.0"], True, id="thousand"),
        pytest.param(["1,000.0"], True, id="financial thousand"),
        pytest.param([" 1,000.0 "], True, id="whitespace"),
        pytest.param(["-1,000.0 "], True, id="negative financial"),
    ],
)
def test_FloatConverter__sniff(inp, expected):
    ic = FloatConverter()
    assert ic.sniff(inp) is expected


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param("1.0", 1.0, id="an int"),
        pytest.param("-1.0", -1.0, id="a negative int"),
        pytest.param("1.", 1.0, id="a float (no trailing 0)"),
        pytest.param("1", 1.0, id="an int (works ok)"),
        pytest.param("1000.0", 1000.0, id="thousand"),
        pytest.param("1,000.0", 1000.0, id="financial thousand"),
        pytest.param(" 1,000.0 ", 1000.0, id="whitespace"),
        pytest.param("-1,000.0 ", -1000.0, id="negative financial"),
        pytest.param(" ", None, id="whitespace"),
        pytest.param("", None, id="blank"),
    ],
)
def test_FloatConverter__convert(inp, expected):
    ic = FloatConverter()
    assert ic.convert(inp) == expected


def test_FloatConverter__convert_failure():
    ic = FloatConverter()
    with pytest.raises(exc.UnconvertableValueException):
        ic.convert("nonsense")


def casings(c):
    return [
        c.upper(),
        c.capitalize(),
        c.lower(),
        f" {c} ",
        f"{c} ",
        f" {c}",
        c[0].upper(),
        c[0].lower(),
    ]


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param([item], True)
        for subl in [casings("true"), casings("false"), casings("yes"), casings("no")]
        for item in subl
    ],
)
def test_BooleanConverter__sniff(inp, expected):
    ic = BooleanConverter()
    assert ic.sniff(inp) is expected


@pytest.mark.parametrize(
    "inp, expected",
    [
        pytest.param(item, True)
        for subl in [casings("true"), casings("yes")]
        for item in subl
    ]
    + [
        pytest.param(item, False)
        for subl in [casings("false"), casings("no")]
        for item in subl
    ],
)
def test_BooleanConverter__convert(inp, expected):
    ic = BooleanConverter()
    assert ic.convert(inp) == expected


def test_BooleanConverter__convert_failure():
    ic = BooleanConverter()
    with pytest.raises(exc.UnconvertableValueException):
        ic.convert("nonsense")


@pytest.mark.parametrize(
    "Converter", [BooleanConverter, DateConverter, FloatConverter, IntegerConverter]
)
@pytest.mark.parametrize(
    "null_str",
    [
        "",
        "#N/A",
        "#N/A N/A",
        "#NA",
        "-1.#IND",
        "-1.#QNAN",
        "-NaN",
        "-nan",
        "1.#IND",
        "1.#QNAN",
        "<NA>",
        "N/A",
        "NA",
        "NULL",
        "NaN",
        "n/a",
        "nan",
        "null",
    ],
)
def test_nulls(Converter, null_str):
    c = Converter()
    assert c.convert(null_str) is None
