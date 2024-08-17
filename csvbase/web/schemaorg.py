"""Functions for producing JSON-LD documents from value objects.

These functions are all in the web tier and can rely on the Flask app context
being pushed.

"""

from typing import Dict, Any, Collection

from flask import url_for

from csvbase.value_objs import Table, TableRepresentation, ColumnType


def to_dataset(table: Table, reps: Collection[TableRepresentation]) -> Dict[str, Any]:
    """Produce a schema.org Dataset object from a Table."""
    obj = {
        "@context": ["https://schema.org", {"csvw": "https://www.w3.org/ns/csvw#"}],
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
        # description is a mandatory field for most
        "description": table.caption if table.has_caption() else "No caption",
        "mainEntity": to_csvw_table(table),
    }

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


CSVW_TYPE_MAP = {
    ColumnType.TEXT: "string",
    ColumnType.INTEGER: "integer",
    ColumnType.FLOAT: "double",
    ColumnType.BOOLEAN: "boolean",
    ColumnType.DATE: "date",
}


def to_csvw_table(table: Table) -> Dict[str, Any]:
    """Produce CSVW ("CSV on the Web") Table from our table."""
    return {
        "@type": "csvw:Table",
        "csvw:tableSchema": {
            "csvw:columns": [
                {"csvw:name": column.name, "csvw:datatype": CSVW_TYPE_MAP[column.type_]}
                for column in table.columns
            ]
        },
    }
