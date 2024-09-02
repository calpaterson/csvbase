import re
import functools

from marko import Markdown, inline
from marko.ext.gfm import elements, renderer
from marko.helpers import MarkoExtension, render_dispatch
from marko.html_renderer import HTMLRenderer

_md = None

# FIXME: pasted yet again
def comment_id_to_page_number(comment_id: int) -> int:
    return ((comment_id - 1) // 10) + 1


class CommentReference(inline.InlineElement):
    """A comment referenced, eg '#10' for comment 10 in this thread."""
    pattern = r"#(\d+)"
    parse_children = False
    children = []

    def __init__(self, match):
        self.comment_id = int(match.group(1))


class CSVBaseRendererMixin:
    @render_dispatch(HTMLRenderer)  # type: ignore
    def render_comment_reference(self, element: CommentReference) -> str:
        """Turns a comment reference into a link"""
        page_number = comment_id_to_page_number(element.comment_id)
        return f'<a href="?page={page_number}#comment-{element.comment_id}">#{element.comment_id}</a>'

CSVBaseExtension = MarkoExtension(
    elements=[CommentReference],
    renderer_mixins=[CSVBaseRendererMixin],
)

class BootstrapRendererMixin(renderer.GFMRendererMixin):
    """Renderer that mainly inherits the original rendering code except
    altering as necessary for Bootstrap."""

    @render_dispatch(HTMLRenderer)
    def render_table(self, element):
        head, *body = element.children
        theader = "<thead>\n{}</thead>".format(self.render(head))  # type: ignore
        tbody = ""
        if body:
            tbody = "\n<tbody>\n{}</tbody>".format(
                "".join(self.render(row) for row in body)  # type: ignore
            )
        return f'<table class="table">\n{theader}{tbody}</table>'

    # FIXME: blockquote should be included here


BootstrapGFM = MarkoExtension(
    elements=[
        elements.Paragraph,
        elements.InlineHTML,
        elements.Strikethrough,
        elements.Url,
        elements.Table,
        elements.TableRow,
        elements.TableCell,
    ],
    renderer_mixins=[BootstrapRendererMixin],
)


def get_markdown():
    global _md
    if _md is None:
        return Markdown(extensions=["codehilite", BootstrapGFM, CSVBaseExtension])
    return _md


# FIXME: use pyappcache
@functools.lru_cache
def render_markdown(md_str: str) -> str:
    return get_markdown().convert(md_str)


QUOTE_REGEX = re.compile(r"^(.*)", re.MULTILINE)


@functools.lru_cache
def quote_markdown(md_str: str) -> str:
    """Prepend '> ' to each line of the input, a markdown blockquote."""
    return re.sub(QUOTE_REGEX, r"> \1", md_str)

REFERENCES_REGEX = re.compile(r"#\d+")


def extract_references(md_str: str) -> list[str]:
    """Pull all references (as they are, textually) out of a comment."""
    return re.findall(REFERENCES_REGEX, md_str)
