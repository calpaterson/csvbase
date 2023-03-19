from pathlib import Path
import io
from werkzeug.datastructures import FileStorage

import pandas as pd
import pytest

from csvbase.value_objs import ContentType

SAMPLE_DATAFRAME = pd.DataFrame({"id": range(3), "value": ["a", "b", "c"]})


@pytest.mark.parametrize(
    "from_format, to_format",
    [
        (ContentType.CSV, ContentType.PARQUET),
        # (ContentType.PARQUET, ContentType.CSV),
    ],
)
def test_convert_file(client, test_user, from_format, to_format):
    get_resp = client.get("/convert")
    assert get_resp.status_code == 200

    buf = io.BytesIO()
    methods = {
        ContentType.CSV: SAMPLE_DATAFRAME.to_csv,
        ContentType.PARQUET: SAMPLE_DATAFRAME.to_parquet,
    }
    methods[from_format](buf)
    buf.seek(0)

    filename = Path("test").with_suffix("." + from_format.file_extension())

    post_resp = client.post(
        "/convert",
        data={
            "from-format": from_format.value,
            "to-format": to_format.value,
            "file": (FileStorage(buf, str(filename))),
        },
        content_type="multipart/form-data",
    )

    assert post_resp.status_code == 200
    expected_filename = filename.with_suffix("." + to_format.file_extension())
    assert (
        post_resp.headers["Content-Disposition"]
        == f'attachment; filename="{expected_filename}"'
    )
