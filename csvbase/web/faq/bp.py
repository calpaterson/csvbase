import re
import importlib_resources
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Iterable

import toml
from flask import Blueprint, make_response, render_template, current_app
from werkzeug.wrappers.response import Response

from csvbase import exc
from csvbase.markdown import render_markdown

bp = Blueprint("faq", __name__)

CACHE_TTL = int(timedelta(days=1).total_seconds())


@dataclass
class FAQEntry:
    slug: str
    title: str
    description: str
    draft: bool
    markdown: str
    created: datetime
    updated: datetime


METADATA_REGEX = re.compile(r"^<!\--(.*)-->", re.MULTILINE | re.DOTALL)


def get_entry(slug: str) -> FAQEntry:
    # entries are written in markdown with a leading HTML comment that includes
    # TOML metadata
    trav = importlib_resources.files("csvbase.web.faq.entries").joinpath(f"{slug}.md")
    try:
        with trav.open("rt") as entry_file:
            markdown = entry_file.read()
    except FileNotFoundError:
        raise exc.FAQEntryDoesNotExistException()
    match_obj = METADATA_REGEX.match(markdown)
    if match_obj is None:
        # should be very hard to hit this as all slugs are tested by enumeration
        raise RuntimeError("unparseable FAQ entry")
    metadata_toml = match_obj.group(1)
    metadata = toml.loads(metadata_toml)

    faq_entry = FAQEntry(
        slug=slug,
        title=metadata["title"],
        description=metadata["description"],
        draft=metadata["draft"],
        markdown=markdown,
        created=metadata["created"],
        updated=metadata["updated"],
    )

    return faq_entry


def get_entries() -> Iterable[FAQEntry]:
    for trav in importlib_resources.files("csvbase.web.faq.entries").iterdir():
        if trav.is_file() and trav.name.endswith(".md"):
            slug = trav.name[:-3]
            yield get_entry(slug)


def set_cache_control(response: Response) -> None:
    """Set the cache control for FAQ entries.

    This is basic, just to stop the markdown being continually regenerated.

    """
    if not current_app.debug:
        cc = response.cache_control
        cc.max_age = CACHE_TTL


@bp.get("/faq")
def faq_index() -> Response:
    resp = make_response(
        render_template("faq/faq-index.html", entries=get_entries(), page_title="FAQ")
    )
    set_cache_control(resp)
    return resp


@bp.get("/faq/<slug>")
def faq_entry(slug: str) -> Response:
    entry = get_entry(slug)
    rendered = render_markdown(entry.markdown)
    resp = make_response(
        render_template(
            "faq/faq-entry.html", entry=entry, rendered=rendered, page_title=entry.title
        )
    )
    set_cache_control(resp)
    return resp
