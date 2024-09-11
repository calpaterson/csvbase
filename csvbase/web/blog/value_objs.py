from datetime import date
from typing import Optional
from uuid import UUID
from dataclasses import dataclass


@dataclass
class Post:
    id: int
    title: str
    uuid: UUID
    description: str
    draft: bool
    markdown: str
    cover_image_url: str
    cover_image_alt: str
    posted: Optional[date] = None
    thread_slug: Optional[str] = None

    def render_posted(self) -> str:
        if self.posted is not None:
            return self.posted.isoformat()
        else:
            return "(not posted yet)"
