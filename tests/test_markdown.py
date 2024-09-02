from textwrap import dedent
from csvbase import markdown


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


def text_extract_references__no_reference():
    assert markdown.extract_references("Hello, World!") == []


def test_extract_references__simple_reference():
    assert markdown.extract_references("Yeah, so about #8 - I think that") == ["#8"]


def test_extract_references__multiple_references():
    inp_markdown = """#8
Yes you're right but what about #9
"""

    assert markdown.extract_references(inp_markdown) == ["#8", "#9"]
