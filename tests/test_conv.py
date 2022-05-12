from datetime import date

import pytest

from csvbase.conv import DateConverter


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
