import re
import functools

from marko import Markdown
from marko.ext.gfm import elements, renderer
from marko.helpers import MarkoExtension, render_dispatch
from marko.html_renderer import HTMLRenderer

_md = None


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
        return Markdown(extensions=["codehilite", BootstrapGFM])
    return _md


# FIXME: use pyappcache
@functools.lru_cache
def render_markdown(md_str: str) -> str:
    return get_markdown().convert(md_str)


QUOTE_REGEX = re.compile(r"^(.*)", re.MULTILINE)


@functools.lru_cache
def quote_markdown(md_str: str) -> str:
    return re.sub(QUOTE_REGEX, r"> \1", md_str)
