import re
import importlib_resources
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Dict, List, Optional

import toml
from flask import Blueprint, make_response, render_template, current_app
from werkzeug.wrappers.response import Response

from csvbase import exc
from csvbase.markdown import render_markdown

bp = Blueprint("faq", __name__)

CACHE_TTL = int(timedelta(days=1).total_seconds())

CATEGORIES: Dict[Optional[str], str] = {
    "basics": "The basics",
    "tools": "Tools",
    None: "Misc",
}


@dataclass
class FAQEntry:
    slug: str
    title: str
    description: str
    category: Optional[str]
    draft: bool
    markdown: str
    created: datetime
    updated: datetime
    order: Optional[int]


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
        category=metadata.get("category", None),
        order=metadata.get("order", 1000),
    )

    return faq_entry


def get_entries() -> List[FAQEntry]:
    entries = []
    for trav in importlib_resources.files("csvbase.web.faq.entries").iterdir():
        if trav.is_file() and trav.name.endswith(".md"):
            slug = trav.name[:-3]
            entries.append(get_entry(slug))

    return sorted(entries, key=lambda e: (e.order or 100, e.slug))


def get_entries_by_category() -> Dict[str, List[FAQEntry]]:
    rv: Dict[str, List[FAQEntry]] = {v: [] for v in CATEGORIES.values()}
    for entry in get_entries():
        rv[CATEGORIES[entry.category]].append(entry)
    return rv


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
        render_template(
            "faq/faq-index.html",
            entries_by_category=get_entries_by_category(),
            page_title="FAQ",
        )
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
