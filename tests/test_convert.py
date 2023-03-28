from pathlib import Path
import io
from werkzeug.datastructures import FileStorage

import pandas as pd
from pandas.testing import assert_frame_equal
import pytest

from csvbase.value_objs import ContentType

SAMPLE_DATAFRAME = pd.DataFrame(
    {"id": [100, 200, 300], "value": ["a", "b", "c"]}
).set_index("id")


@pytest.mark.parametrize(
    "from_format, to_format",
    [
        (ContentType.CSV, ContentType.CSV),
        (ContentType.CSV, ContentType.PARQUET),
        (ContentType.CSV, ContentType.XLSX),
        (ContentType.PARQUET, ContentType.CSV),
    ],
)
def test_convert__a_to_b(client, test_user, from_format, to_format):
    methods = {
        ContentType.CSV: SAMPLE_DATAFRAME.to_csv,
        ContentType.PARQUET: SAMPLE_DATAFRAME.to_parquet,
    }
    reverse_methods = {
        ContentType.CSV: pd.read_csv,
        ContentType.PARQUET: pd.read_parquet,
        ContentType.XLSX: pd.read_excel
    }

    get_resp = client.get("/convert")
    assert get_resp.status_code == 200

    buf = io.BytesIO()
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

    actual_dataframe = reverse_methods[to_format](io.BytesIO(post_resp.data)).set_index(
        "id"
    )
    assert_frame_equal(SAMPLE_DATAFRAME, actual_dataframe)


@pytest.mark.xfail(reason="not implemented")
def test_convert__unreadable_file():
    assert False


@pytest.mark.xfail(reason="not implemented")
def test_convert__unknown_content_type():
    assert False
