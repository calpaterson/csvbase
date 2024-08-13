from lxml import etree
from datetime import datetime

from csvbase.web import schemaorg
from .utils import test_data_path


def test_robots(client):
    resp = client.get("/robots.txt")
    assert resp.status_code == 200
    assert resp.data == b"Sitemap: http://localhost/sitemap.xml"
    assert resp.headers["Cache-Control"] == "public, max-age=86400"


def test_sitemap(client):
    resp = client.get("/sitemap.xml")
    assert resp.status_code == 200
    assert resp.headers["Cache-Control"] == "public, max-age=86400"

    # Check it's valid XML
    with open(test_data_path + "/sitemap.xsd", "rb") as sitemap_xsd_f:
        schema_root = etree.XML(sitemap_xsd_f.read())
    schema = etree.XMLSchema(schema_root)
    parser = etree.XMLParser(schema=schema)

    # for some reason this line often flakes on CI, still investigating
    root = etree.XML(resp.data, parser), resp.data
    assert root is not None

    # Double check this easy-to-create issue
    first_line = resp.data.splitlines()[0]
    assert first_line == b"<?xml version='1.0' encoding='UTF-8'?>"


def test_schemaorg_dataset(ten_rows):
    expected = {
        "@context": "https://schema.org",
        "@type": "Dataset",
        "name": ten_rows.table_name,
        "description": ten_rows.caption,
        "url": f"http://localhost/{ten_rows.username}/{ten_rows.table_name}",
        "isAccessibleForFree": True,
        "distribution": [
            {
                "@type": "DataDownload",
                "contentUrl": f"http://localhost/{ten_rows.username}/{ten_rows.table_name}.csv",
                "encodingFormat": "text/csv",
            },
            {
                "@type": "DataDownload",
                "contentUrl": f"http://localhost/{ten_rows.username}/{ten_rows.table_name}.parquet",
                "encodingFormat": "application/parquet",
            },
            {
                "@type": "DataDownload",
                "contentUrl": f"http://localhost/{ten_rows.username}/{ten_rows.table_name}.jsonl",
                "encodingFormat": "application/x-jsonlines",
            },
            {
                "@type": "DataDownload",
                "contentUrl": f"http://localhost/{ten_rows.username}/{ten_rows.table_name}.xlsx",
                "encodingFormat": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            },
        ],
        "publisher": {
            "@type": "Organization",
            "logo": "http://localhost/static/logo/192x192.png",
            "name": "csvbase",
            "url": "http://localhost/",
        },
        "maintainer": {
            "@type": "Person",
            "name": ten_rows.username,
            "url": f"http://localhost/{ten_rows.username}",
        },
    }
    actual = schemaorg.to_dataset(ten_rows)

    # do the dates this way
    assert datetime.fromisoformat(actual.pop("dateCreated")) == ten_rows.created  # type: ignore
    assert datetime.fromisoformat(actual.pop("dateModified")) == ten_rows.last_changed  # type: ignore

    # the rest must match:
    assert expected == actual
