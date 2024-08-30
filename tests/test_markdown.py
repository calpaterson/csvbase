from textwrap import dedent
from csvbase import markdown

import pytest

def test_quote():
    input_string = dedent(
        """\
hello

this is
a test

of quoting"""
    )

    expected = dedent(
        """\
> hello
>
> this is
> a test
>
> of quoting"""
    )

    actual = markdown.quote_markdown(input_string)

    # doing this makes this file less sensitive to trailing newlines
    for actual_line, expected_line in zip(actual.splitlines(), expected.splitlines()):
        assert actual_line.strip() == expected_line.strip()


def test_pop_references__no_reference():
    assert markdown.pop_references("Hello, World!") == ([], "Hello, World!")

@pytest.mark.parametrize("references_str, expected_references", [
    pytest.param("3", ["3"], id="one reference"),
    pytest.param("3 2", ["3", "2"], id="two references"),
    pytest.param("3 stock-exchanges/rows/3", ["3", "stock-exchanges/rows/3"], id="rows"),
])
def test_pop_references__one_reference(references_str, expected_references):
    inp = f"""References: {references_str}
Hello, World!"""

    references, md_str = markdown.pop_references(inp)
    assert references == expected_references
    assert md_str == "Hello, World!"
