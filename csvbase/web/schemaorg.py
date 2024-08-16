from typing import Dict, Any, Collection

from flask import url_for

from csvbase.value_objs import Table, TableRepresentation


def to_dataset(table: Table, reps: Collection[TableRepresentation]) -> Dict[str, Any]:
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
        "isAccessibleForFree": table.is_public,
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
    for rep in reps:
        distribution.append(to_datadownload(table, rep))
    obj["distribution"] = distribution

    return obj


def to_datadownload(table: Table, rep: TableRepresentation) -> Dict[str, str]:
    """Produce a schema.org DataDownload object from a table + content type."""
    obj = {
        "@type": "DataDownload",
        "contentUrl": url_for(
            "csvbase.table_view_with_extension",
            username=table.username,
            table_name=table.table_name,
            extension=rep.content_type.file_extension(),
            _external=True,
        ),
        "encodingFormat": rep.content_type.value,
    }
    if not rep.size_is_estimate:
        obj["contentSize"] = str(rep.size)
    return obj


def make_organisation() -> Dict[str, str]:
    """Produce the schema.org Publisher object for this csvbase instance."""
    return {
        "@type": "Organization",
        "name": "csvbase",
        "url": url_for("csvbase.index", _external=True),
        "logo": url_for("static", filename="logo/192x192.png", _external=True),
    }


def to_person(username: str) -> Dict[str, str]:
    """Produce the schema.org Person object for this username."""
    # This is quite basic
    return {
        "@type": "Person",
        "name": username,
        "url": url_for("csvbase.user", username=username, _external=True),
    }
