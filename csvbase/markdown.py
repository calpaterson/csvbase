import functools

from marko import Markdown


_md = None


def get_markdown():
    global _md
    if _md is None:
        return Markdown(extensions=["codehilite"])
    return _md


# FIXME: use pyappcache
@functools.lru_cache
def render_markdown(md_str: str) -> str:
    return get_markdown().convert(md_str)
