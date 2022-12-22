import functools

from marko import Markdown


@functools.lru_cache
def get_markdown():
    return Markdown(extensions=["codehilite"])


def render_markdown(md_str: str) -> str:
    return get_markdown().convert(md_str)
