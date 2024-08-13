from typing import Mapping, Any

from flask import url_for

from csvbase.value_objs import Table, ContentType


def to_dataset(table: Table) -> Mapping[str, Any]:
    """Produce a schema.org Dataset object from a Table."""
    obj = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": table.table_name,
        "url": url_for(
            "csvbase.table_view",
            username=table.username,
            table_name=table.table_name,
            _external=True,
        ),
        "isAccessibleForFree": True,
        "distribution": [],
        "dateCreated": table.created.isoformat(),
        "dateModified": table.last_changed.isoformat(),
        "publisher": make_organisation(),
        "maintainer": to_person(table.username),
    }
    if table.has_caption():
        obj["description"] = table.caption

    # Mark up all the reps we hold
    distribution = []
    content_types = [ContentType.CSV, ContentType.PARQUET, ContentType.JSON_LINES]
    if not table.row_count.is_big():
        content_types.append(ContentType.XLSX)
    for content_type in content_types:
        distribution.append(to_datadownload(table, content_type))
    obj["distribution"] = distribution

    return obj


def to_datadownload(table: Table, content_type: ContentType) -> Mapping[str, str]:
    """Produce a schema.org DataDownload object from a table + content type."""
    # potential improvements:
    # contentSize (needs the rep)
    obj = {
        "@type": "DataDownload",
        "contentUrl": url_for(
            "csvbase.table_view_with_extension",
            username=table.username,
            table_name=table.table_name,
            extension=content_type.file_extension(),
            _external=True,
        ),
        "encodingFormat": content_type.value,
    }
    return obj


def make_organisation() -> Mapping[str, str]:
    """Produce the schema.org Publisher object for this csvbase instance."""
    return {
        "@type": "Organization",
        "name": "csvbase",
        "url": url_for("csvbase.index", _external=True),
        "logo": url_for("static", filename="logo/192x192.png", _external=True),
    }


def to_person(username: str) -> Mapping[str, str]:
    """Produce the schema.org Person object for this username."""
    # This is quite basic
    return {
        "@type": "Person",
        "name": username,
        "url": url_for("csvbase.user", username=username, _external=True),
    }
