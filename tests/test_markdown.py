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
