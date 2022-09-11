from datetime import date
from typing import Optional
from uuid import UUID
from dataclasses import dataclass

import marko


@dataclass
class Post:
    slug: str
    title: str
    uuid: UUID
    description: str
    draft: bool
    markdown: str
    posted: Optional[date] = None
    cover_image: Optional[str] = None
    cover_image_alt: Optional[str] = None

    def render_markdown(self) -> str:
        return marko.convert(self.markdown)

    def render_posted(self) -> str:
        if self.posted is not None:
            return self.posted.isoformat()
        else:
            return "(not posted yet)"
