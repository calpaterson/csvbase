from io import BytesIO
from werkzeug.datastructures import FileStorage

import pytest


@pytest.mark.xfail(reason="not implemented")
def test_convert_file(client, test_user):
    get_resp = client.get("/convert")
    assert get_resp.status_code == 200

    post_resp = client.post(
        "/convert",
        data={
            "from-format": "CSV",
            "to-format": "PARQUET",
            "file": (FileStorage(BytesIO(b"a,b,c\n1,2,3"), "test.csv")),
        },
        content_type="multipart/form-data",
    )

    assert post_resp.status_code == 302

    download_page_resp = client.get(post_resp["Location"])
    assert download_page_resp.status_code == 200

    # FIXME: next check the download link
