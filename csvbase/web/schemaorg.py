from typing import Mapping, Any

from flask import url_for

from csvbase.value_objs import Table, ContentType


def to_dataset(table: Table) -> Mapping[str, Any]:
    """Produce a schema.org Dataset object from a Table."""
    # potential improvements:
    # maintainer
    # publisher
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
    }
    if table.has_caption():
        obj["description"] = table.caption
    # if we knew the table wasn't big we could refer to XLSX here:
    for content_type in [ContentType.CSV, ContentType.PARQUET, ContentType.JSON_LINES]:
        obj["distribution"].append(to_datadownload(table, content_type))

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
