import io
from uuid import UUID
from pathlib import Path
import shutil
from datetime import date, timedelta, timezone
from logging import getLogger
from typing import (
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Union,
    cast,
    Iterator,
    IO,
)
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import hashlib

from dateutil.zoneinfo import get_zonefile_instance
import itsdangerous.url_safe
from werkzeug.datastructures import OrderedMultiDict
import werkzeug.http
from flask import (
    Blueprint,
    current_app,
    flash,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
    url_for,
)
from flask.views import MethodView
from flask import session as flask_session
from flask_cors import cross_origin, CORS
from typing_extensions import Literal
from werkzeug.wrappers.response import Response
from werkzeug.wrappers.request import ImmutableMultiDict

from ..func import (
    is_browser,
    set_current_user,
    get_current_user_or_401,
    get_current_user,
    reverse_url_for,
)
from ... import exc, svc, streams, table_io
from ...json import value_to_json, json_to_value
from ...markdown import render_markdown
from ...sesh import get_sesh
from ...userdata import PGUserdataAdapter
from ...conv import DateConverter, IntegerConverter, FloatConverter
from ...value_objs import (
    ROW_ID_COLUMN,
    Column,
    ColumnType,
    ContentType,
    Encoding,
    DataLicence,
    KeySet,
    Page,
    PythonType,
    Row,
    Table,
    User,
)
from ...streams import UserSubmittedCSVData
from ...constants import COPY_BUFFER_SIZE
from ..billing import svc as billing_svc

logger = getLogger(__name__)

bp = Blueprint("csvbase", __name__)

CORS_EXPIRY = timedelta(hours=8)

CORS(
    bp,
    resources={
        r"/[A-Za-z][-A-Za-z0-9]+/[A-Za-z][-A-Za-z0-9]+": {
            "origins": "*",
            "methods": ["GET", "PUT", "POST", "DELETE"],
            "max_age": CORS_EXPIRY,
        }
    },
)


@bp.route("/")
def index() -> str:
    sesh = get_sesh()
    return render_template("index.html", top_ten=svc.get_top_n(sesh))


@bp.route("/about")
def about() -> str:
    return render_template(
        "about.html", page_title="About csvbase, a simple web database"
    )


class ConvertForm(MethodView):
    def get(self) -> Response:
        return make_response(
            render_template(
                "convert.html",
                input_formats=[
                    ContentType.CSV,
                    # FIXME: Parquet's type system needs more work to be able
                    # to parse it in csvbase
                    # ContentType.PARQUET,
                ],
                output_formats=[
                    ContentType.CSV,
                    ContentType.PARQUET,
                    ContentType.XLSX,
                    ContentType.JSON_LINES,
                ],
                default_output_format=ContentType.PARQUET,
                default_input_format=ContentType.CSV,
            )
        )

    def post(self) -> Response:
        to_content_type = ContentType(request.form["to-format"])
        from_content_type = ContentType(request.form["from-format"])

        if request.files["file"].filename is not None:
            original_filename = Path(request.files["file"].filename)
        else:
            original_filename = Path("converted")
        converted_filename = original_filename.with_suffix(
            "." + to_content_type.file_extension()
        )

        if from_content_type == ContentType.CSV:
            str_buf = streams.byte_buf_to_str_buf(request.files["file"])
            dialect, columns = streams.peek_csv(str_buf)
            rows = table_io.csv_to_rows(str_buf, columns, dialect)
        elif from_content_type == ContentType.PARQUET:
            pf = table_io.buf_to_pf(cast(IO[bytes], request.files["file"]))
            columns = table_io.parquet_file_to_columns(pf)
            rows = table_io.parquet_file_to_rows(pf)

        if to_content_type == ContentType.PARQUET:
            response_buf = table_io.rows_to_parquet(columns, rows)
        elif to_content_type == ContentType.CSV:
            response_buf = table_io.rows_to_csv(columns, rows)
        elif to_content_type == ContentType.XLSX:
            response_buf = table_io.rows_to_xlsx(columns, rows)
        elif to_content_type == ContentType.JSON_LINES:
            response_buf = table_io.rows_to_jsonlines(columns, rows)
        else:
            raise exc.InvalidRequest()

        return make_streaming_response(
            response_buf,
            download_filename=str(converted_filename),
        )


bp.add_url_rule("/convert", "convert", view_func=ConvertForm.as_view("convert-form"))


@bp.route("/new-table/paste")
def paste() -> str:
    return render_template(
        "new-table.html",
        method="paste",
        DataLicence=DataLicence,
        action_url=url_for("csvbase.new_table_form_submission"),
        page_title="Paste a new table",
    )


@bp.get("/new-table/upload-file")
def upload_file() -> str:
    return render_template(
        "new-table.html",
        method="upload-file",
        DataLicence=DataLicence,
        Encoding=Encoding,
        action_url=url_for("csvbase.new_table_form_submission"),
        page_title="Upload a new table",
    )


@bp.post("/new-table")
def new_table_form_submission() -> Response:
    sesh = get_sesh()
    if "username" in request.form:
        current_user = register_and_sign_in_new_user(sesh, request.form)
    else:
        current_user = get_current_user_or_401()

    quota = billing_svc.get_quota(sesh, current_user.user_uuid)
    usage = svc.get_usage(sesh, current_user.user_uuid)
    private = "private" in request.form
    if private:
        usage.private_tables += 1
    else:
        usage.public_tables += 1
    if usage.exceeds_quota(quota):
        logger.warning("%s tried to exceed quota", user)
        raise exc.NotEnoughQuotaException()

    table_name = request.form["table-name"]
    csv_buf: UserSubmittedCSVData

    is_public = not private
    data_licence = DataLicence(request.form.get("data-licence", type=int))
    table_uuid = svc.create_table_metadata(
        sesh,
        current_user.user_uuid,
        table_name,
        is_public,
        "",
        data_licence,
    )

    textarea = request.form.get("csv-textarea")
    if textarea:
        csv_buf = io.StringIO(textarea)
    else:
        byte_buf = request.files["csv-file"]
        encoding = request.form.get("encoding", type=Encoding)
        csv_buf = streams.byte_buf_to_str_buf(byte_buf, encoding)

    try:
        dialect, columns = streams.peek_csv(csv_buf)
        PGUserdataAdapter.create_table(sesh, table_uuid, columns)
        table = svc.get_table(sesh, current_user.username, table_name)
        rows = table_io.csv_to_rows(csv_buf, columns, dialect)
    except UnicodeDecodeError as e:
        raise exc.WrongEncodingException() from e

    PGUserdataAdapter.insert_table_data(
        sesh,
        table,
        columns,
        rows,
    )
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()
    return redirect(
        url_for(
            "csvbase.table_view",
            username=current_user.username,
            table_name=table_name,
        )
    )


@bp.get("/new-table/blank")
def blank_table() -> str:
    def build_cols(args) -> List[Tuple[str, ColumnType]]:
        index = 1
        cols = []
        while True:
            try:
                col_name = request.args[f"col-name-{index}"]
                col_type = ColumnType[request.args[f"col-type-{index}"]]
                cols.append((col_name, col_type))
            except KeyError:
                break
            index += 1

        if cols == []:
            cols.append(("", ColumnType.TEXT))

        if "add_col" in request.args:
            cols.append(("", ColumnType.TEXT))

        remove_col = request.args.get("remove_col", type=int)
        if remove_col is not None:
            del cols[remove_col - 1]

        return cols

    cols = build_cols(request.args)
    table_name = request.args.get("table-name", "")

    return render_template(
        "new-blank-table.html",
        action_url=url_for("csvbase.blank_table_form_post"),
        DataLicence=DataLicence,
        cols=cols,
        ColumnType=ColumnType,
        table_name=table_name,
    )


@bp.post("/new-table/blank")
def blank_table_form_post() -> Response:
    sesh = get_sesh()
    if "username" in request.form:
        current_user = register_and_sign_in_new_user(sesh, request.form)
    else:
        current_user = get_current_user_or_401()

    quota = billing_svc.get_quota(sesh, current_user.user_uuid)
    usage = svc.get_usage(sesh, current_user.user_uuid)
    if "private" in request.form:
        usage.private_tables += 1
    else:
        usage.public_tables += 1
    if usage.exceeds_quota(quota):
        logger.warning("%s tried to exceed quota", user)
        raise exc.NotEnoughQuotaException()

    cols = []
    index = 1
    while True:
        try:
            col_name = request.form[f"col-name-{index}"]
            col_type = ColumnType[request.form[f"col-type-{index}"]]
            cols.append(Column(col_name, col_type))
        except KeyError:
            break
        index += 1
    table_name = request.form["table-name"]
    licence = DataLicence(request.form.get("data-licence", type=int))
    if "private" in request.form:
        is_public = False
    else:
        is_public = True

    table_uuid = svc.create_table_metadata(
        sesh,
        current_user.user_uuid,
        table_name,
        is_public,
        "",
        licence,
    )
    PGUserdataAdapter.create_table(sesh, table_uuid, cols)
    sesh.commit()
    return redirect(
        url_for(
            "csvbase.table_view",
            username=current_user.username,
            table_name=table_name,
        )
    )


class TableView(MethodView):
    """This covers the "table API" plus the HTML "View" page."""

    def get(self, username: str, table_name: str) -> Response:
        """Get a table"""
        sesh = get_sesh()
        user = svc.user_by_name(sesh, username)
        if not svc.is_public(sesh, username, table_name) and not am_user(user.username):
            raise exc.TableDoesNotExistException(username, table_name)

        content_type = negotiate_content_type(
            [ContentType.HTML, ContentType.JSON], default=ContentType.CSV
        )

        table = svc.get_table(sesh, username, table_name)

        return make_table_view_response(sesh, content_type, table)

    def put(self, username: str, table_name: str) -> Response:
        """Create or overwrite a table."""
        sesh = get_sesh()
        am_user_or_400(username)
        user = svc.user_by_name(sesh, username)

        response_content_type = negotiate_content_type(
            [ContentType.JSON], default=ContentType.JSON
        )

        # FIXME: add checking for forms here
        byte_buf = io.BytesIO()
        shutil.copyfileobj(request.stream, byte_buf)
        str_buf = streams.byte_buf_to_str_buf(byte_buf)

        if svc.table_exists(sesh, user.user_uuid, table_name):
            table = svc.get_table(sesh, username, table_name)
            dialect, csv_columns = streams.peek_csv(str_buf, table.columns)
            rows = table_io.csv_to_rows(str_buf, csv_columns, dialect)

            # If there is no csvbase_row_id column, don't try to correlate
            # updates, just wipe the table and insert everything.
            if "csvbase_row_id" not in set(c.name for c in csv_columns):
                PGUserdataAdapter.delete_table_data(sesh, table)
                PGUserdataAdapter.insert_table_data(sesh, table, csv_columns, rows)
            else:
                PGUserdataAdapter.upsert_table_data(
                    sesh,
                    table,
                    csv_columns,
                    rows,
                )
            status = 200
            message = f"upserted {username}/{table_name}"
        else:
            dialect, csv_columns = streams.peek_csv(str_buf)
            rows = table_io.csv_to_rows(str_buf, csv_columns, dialect)
            is_public = request.args.get("public", default=False, type=bool)
            table_uuid = svc.create_table_metadata(
                sesh,
                user.user_uuid,
                table_name,
                is_public,
                "",
                DataLicence.ALL_RIGHTS_RESERVED,
            )
            PGUserdataAdapter.create_table(sesh, table_uuid, csv_columns)
            table = svc.get_table(sesh, username, table_name)
            PGUserdataAdapter.insert_table_data(sesh, table, csv_columns, rows)
            status = 201
            message = f"created {username}/{table_name}"
        svc.mark_table_changed(sesh, table.table_uuid)
        sesh.commit()
        response = jsonify({"message": message})
        response.status_code = status
        return response

    def delete(self, username: str, table_name: str) -> Response:
        """Create or overwrite a table."""
        sesh = get_sesh()
        am_user_or_400(username)
        svc.get_table(sesh, username, table_name)
        svc.delete_table_and_metadata(sesh, username, table_name)
        sesh.commit()

        message = f"deleted {username}/{table_name}"
        response = jsonify({"message": message})
        response.status_code = 204
        return response

    def post(self, username: str, table_name: str) -> Response:
        """Append some new rows to a table."""
        sesh = get_sesh()
        am_user_or_400(username)
        table = svc.get_table(sesh, username, table_name)

        response_content_type = negotiate_content_type(
            [ContentType.JSON], default=ContentType.JSON
        )

        # FIXME: add checking for forms here
        byte_buf = io.BytesIO()
        shutil.copyfileobj(request.stream, byte_buf)
        str_buf = streams.byte_buf_to_str_buf(byte_buf)
        dialect, columns = streams.peek_csv(str_buf, table.columns)
        rows = table_io.csv_to_rows(str_buf, columns, dialect)

        # FIXME: check that columns is a subset of table_columns
        # table_columns = PGUserdataAdapter.get_columns(sesh, columns)

        PGUserdataAdapter.insert_table_data(sesh, table, columns, rows)

        message = f"Updated {username}/{table_name}"
        response = jsonify({"message": message})
        response.status_code = 204
        return response


bp.add_url_rule("/<username>/<table_name>", view_func=TableView.as_view("table_view"))


@bp.post("/<username>/<table_name:table_name>/delete-table-form-post")
def delete_table_form_post(username: str, table_name: str) -> Response:
    """Delete a table, from a form post.

    For now, there is no REST API for this.

    """
    sesh = get_sesh()
    svc.user_by_name(sesh, username)
    svc.get_table(sesh, username, table_name)
    am_user_or_400(username)
    svc.delete_table_and_metadata(sesh, username, table_name)
    sesh.commit()
    flash(f"Deleted {username}/{table_name}")
    return redirect(url_for("csvbase.user", username=username))


CSV_SEPARATOR_MAP: Mapping[str, str] = {"comma": ",", "tab": "\t", "vertical-bar": "|"}


@bp.get("/<username>/<table_name:table_name>.<extension>")
@bp.get("/<username>/<table_name:table_name>/export/<extension>")
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT"])
def table_view_with_extension(
    username: str, table_name: str, extension: str
) -> Response:
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(user.username):
        raise exc.TableDoesNotExistException(username, table_name)

    content_type = ContentType.from_file_extension(extension)
    if content_type is None:
        raise exc.CantNegotiateContentType([e for e in ContentType])

    table = svc.get_table(sesh, username, table_name)

    return make_table_view_response(sesh, content_type, table)


def ensure_not_over_the_top(table: Table, keyset: KeySet, page: Page) -> None:
    """Raise an exception if this page is either over the top or under the
    bottom - but not if it's just an empty table.

    This is used to detect when someone has paged too far, usually by url-editing.

    """
    if (page.has_less or page.has_more) and len(page.rows) == 0:
        raise exc.PageDoesNotExistException(table.username, table.table_name, keyset)


def make_table_view_response(sesh, content_type: ContentType, table: Table) -> Response:
    """Build a representation of a table for a content-type and return a
    response ready to be returned from a handler."""
    keyset = keyset_from_request_args()
    etag = make_table_view_etag(table, content_type, keyset)

    # First, check if we can early-exit without doing anything based on etags
    if_none_match = request.headers.get("If-None-Match", None)
    if if_none_match == etag:
        logger.debug("matched etag (%s), returning 304", etag)
        response = Response(status=304)
        return add_table_metadata_headers(
            table, add_table_view_cache_headers(response, etag)
        )
    elif if_none_match is not None:
        logger.info(
            "provided etag (%s) doesn't match current (%s)", if_none_match, etag
        )
    else:
        logger.debug("no matching etag")

    # If the representation is page based:
    if content_type in {ContentType.HTML, ContentType.JSON}:
        page = PGUserdataAdapter.table_page(sesh, table, keyset)
        ensure_not_over_the_top(table, keyset, page)
        if content_type is ContentType.HTML:
            min_row_id, max_row_id = PGUserdataAdapter.row_id_bounds(
                sesh, table.table_uuid
            )
            row_ids = page.row_ids()
            is_first_page = min_row_id is None or (min_row_id in row_ids)
            is_last_page = max_row_id is None or (max_row_id in row_ids)

            template_kwargs = dict(
                page_title=f"{table.username}/{table.table_name}",
                table=table,
                page=page,
                keyset=keyset,
                ROW_ID_COLUMN=ROW_ID_COLUMN,
                praise_id=get_praise_id_if_exists(table),
                is_first_page=is_first_page,
                is_last_page=is_last_page,
                max_row_id=max_row_id,
                highlight=request.args.get("highlight", None, type=int),
            )

            if is_first_page:
                template_kwargs["readme_html"] = readme_html(sesh, table.table_uuid)

            response = make_response(
                render_template(
                    "table_view.html",
                    **template_kwargs,
                )
            )
            # HTML doesn't get an etag - too hard to key everything that goes in
            return add_table_metadata_headers(
                table, add_table_view_cache_headers(response)
            )
        else:
            return add_table_metadata_headers(
                table,
                add_table_view_cache_headers(
                    jsonify(table_to_json_dict(table, page)), etag
                ),
            )

    # If the representation is whole-table:
    columns = PGUserdataAdapter.get_columns(sesh, table.table_uuid)
    rows = PGUserdataAdapter.table_as_rows(sesh, table.table_uuid)
    if content_type is ContentType.PARQUET:
        streaming_response = make_streaming_response(
            table_io.rows_to_parquet(columns, rows)
        )
    elif content_type is ContentType.JSON_LINES:
        streaming_response = make_streaming_response(
            table_io.rows_to_jsonlines(columns, rows)
        )
    elif content_type is ContentType.XLSX:
        excel_table = "excel-table" in request.args
        xlsx_buf = table_io.rows_to_xlsx(columns, rows, excel_table=excel_table)
        streaming_response = make_streaming_response(
            xlsx_buf,
            ContentType.XLSX,
            make_download_filename(table.username, table.table_name, "xlsx"),
        )
    else:
        # text/csv by default
        separator = request.args.get("separator", "comma")
        try:
            delimiter = CSV_SEPARATOR_MAP[separator]
        except KeyError:
            raise exc.InvalidRequest(f"invalid separator: {separator}")

        extension = "tsv" if separator == "tab" else "csv"
        streaming_response = make_streaming_response(
            table_io.rows_to_csv(columns, rows, delimiter=delimiter),
            ContentType.CSV,
            make_download_filename(table.username, table.table_name, extension),
        )

    with_cache_headers = add_table_view_cache_headers(streaming_response, etag)
    with_metadata_headers = add_table_metadata_headers(table, with_cache_headers)
    return with_metadata_headers


def keyset_to_dict(keyset: KeySet) -> Dict:
    return {
        "columns": [c.name for c in keyset.columns],
        "values": keyset.values,
        "op": keyset.op,
        "size": keyset.size,
    }


def make_table_view_etag(
    table: Table, content_type: ContentType, keyset: KeySet
) -> str:
    current_username = getattr(g, "current_username", "anonymous")
    # we have to hash here because some browsers (eg Chrome) don't seem to
    # handle some characters (eg comma) well in the ETag header
    hash_ = hashlib.blake2b()
    hash_.update(table.table_uuid.bytes)
    hash_.update(str(keyset_to_dict(keyset)).encode("utf-8"))
    hash_.update(content_type.value.encode("utf-8"))
    hash_.update(table.last_changed.isoformat().encode("utf-8"))
    if content_type == ContentType.HTML:
        hash_.update(current_username.encode("utf-8"))
    key = hash_.hexdigest()
    # and we sign to avoid people fishing for other people's cache'd versions
    # with etags
    serializer = itsdangerous.url_safe.URLSafeSerializer(
        current_app.config["SECRET_KEY"]
    )
    etag_key = cast(str, serializer.dumps(key))
    etag = f'W/"{etag_key}"'
    return etag


def add_table_metadata_headers(table: Table, response: Response) -> Response:
    """Add Link and Last-Modified, which are useful out-of-band information for
    consumers.

    """
    # the HTTP spec says only GMT is allowed
    last_changed = table.last_changed.astimezone(timezone.utc)
    url = url_for(
        "csvbase.table_view",
        username=table.username,
        table_name=table.table_name,
        _external=True,
    )
    response.headers["Link"] = f'<{ url }>, rel="canonical"'
    # setting the 'Last-Modified' seems to make varnish get it badly wrong, so
    # set this for now
    response.headers["CSVBase-Last-Modified"] = last_changed.strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    return response


def add_table_view_cache_headers(
    response: Response, etag: Optional[str] = None
) -> Response:
    """Set the ETag and xkey (varnish) cache headers relevant to table views."""
    # Don't set private here - that should already have been done above (and we
    # don't know, here, whether a given table is private or not)

    if etag is not None:
        response.headers["ETag"] = etag

    # Setting max_age to 0 indicates that this response is always stale and
    # should be revalidated
    response.cache_control.max_age = 0

    # HTML views show usernames, other personal data, and we have to indicate
    # if it's an HTML view that the Cookie header is part of the cache key
    if response.mimetype == ContentType.HTML.value:
        response.cache_control.private = True
        response.headers["Vary"] = "Accept, Cookie"
    else:
        response.headers["Vary"] = "Accept"

    # xkeys are used by varnish to do invalidation - currently not used because
    # etags works well enough for now
    # response.headers["xkey"] = "table/{table_uuid}"
    return response


@bp.get("/<username>/<table_name:table_name>/readme")
def table_readme(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    if not svc.is_public(sesh, username, table_name) and not am_user(username):
        raise exc.TableDoesNotExistException(username, table_name)

    table = svc.get_table(sesh, username, table_name)

    readme_markdown = svc.get_readme_markdown(sesh, table.table_uuid)

    if readme_markdown is not None:
        readme_html = render_markdown(readme_markdown)
    else:
        readme_html = "(no readme set)"

    return make_response(
        render_template(
            "table_readme.html",
            page_title=f"Readme for {username}/{table_name}",
            table=table,
            table_readme=readme_html,
            praise_id=get_praise_id_if_exists(table),
        )
    )


@bp.get("/<username>/<table_name:table_name>/docs")
def get_table_apidocs(username: str, table_name: str) -> str:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    table.is_public or am_user_or_400(username)
    owner = svc.user_by_name(sesh, username)

    made_up_row = svc.get_a_made_up_row(sesh, table.table_uuid)
    sample_row = PGUserdataAdapter.get_a_sample_row(sesh, table.table_uuid)
    sample_page = Page(has_less=False, has_more=True, rows=[sample_row])

    return render_template(
        "table_api.html",
        page_title=f"REST docs: {username}/{table_name}",
        owner=owner,
        table=table,
        sample_row=sample_row,
        sample_row_id=row_id_from_row(sample_row),
        sample_page=sample_page,
        made_up_row=made_up_row,
        row_to_json_dict=row_to_json_dict,
        table_to_json_dict=table_to_json_dict,
        url_for_with_auth=url_for_with_auth,
        praise_id=get_praise_id_if_exists(table),
    )


@bp.get("/<username>/<table_name:table_name>/export")
def table_export(username: str, table_name: str) -> str:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    table.is_public or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    table_url = url_for(
        "csvbase.table_view", username=username, table_name=table_name, _external=True
    )
    scheme, public_netloc, path, _, _ = urlsplit(table_url)
    if am_user(username):
        url_username = user.username
        url_hex_key = user.hex_api_key()
    else:
        url_username = "your_username"
        url_hex_key = "your_api_key"
    private_table_url = f"{scheme}://{url_username}:{url_hex_key}@{public_netloc}{path}"

    # if the table is not public the user will need basic auth to get it
    if not table.is_public:
        table_url = private_table_url

    return render_template(
        "table_export.html",
        page_title=f"Export: {username}/{table_name}",
        table=table,
        table_url=table_url,
        private_table_url=private_table_url,
        praise_id=get_praise_id_if_exists(table),
    )


class CopyView(MethodView):
    """For making copies of tables."""

    def get(self, username: str, table_name: str) -> Response:
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        response = make_response(
            render_template(
                "copy.html", table=table, page_title=f"Copy {username}/{table_name}"
            )
        )
        return response

    def post(self, username: str, table_name: str) -> Response:
        sesh = get_sesh()

        if "username" in request.form:
            current_user = register_and_sign_in_new_user(sesh, request.form)
        else:
            current_user = get_current_user_or_401()

        # FIXME: again, this is copied
        quota = billing_svc.get_quota(sesh, current_user.user_uuid)
        usage = svc.get_usage(sesh, current_user.user_uuid)
        private = "private" in request.form
        if private:
            usage.private_tables += 1
        else:
            usage.public_tables += 1
        if usage.exceeds_quota(quota):
            logger.warning("%s tried to exceed quota", user)
            raise exc.NotEnoughQuotaException()

        existing_table = svc.get_table(sesh, username, table_name)
        new_table_name = request.form["table-name"]

        is_public = not private
        new_table_uuid = svc.create_table_metadata(
            sesh,
            current_user.user_uuid,
            new_table_name,
            is_public,
            existing_table.caption,
            existing_table.data_licence,
        )

        PGUserdataAdapter.create_table(sesh, new_table_uuid, existing_table.columns)
        PGUserdataAdapter.copy_table_data(
            sesh, existing_table.table_uuid, new_table_uuid
        )
        svc.record_copy(sesh, existing_table.table_uuid, new_table_uuid)
        svc.mark_table_changed(sesh, new_table_uuid)
        sesh.commit()
        return redirect(
            url_for(
                "csvbase.table_view",
                username=current_user.username,
                table_name=new_table_name,
            )
        )


bp.add_url_rule(
    "/<username>/<table_name>/copy", view_func=CopyView.as_view("copy_view")
)


@bp.get("/<username>/<table_name:table_name>/details")
def table_details(username: str, table_name: str) -> str:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    table.is_public or am_user_or_400(username)

    return render_template(
        "table_details.html",
        username=username,
        page_title=f"Schema & Details: {username}/{table_name}",
        DataLicence=DataLicence,
        table=table,
        praise_id=get_praise_id_if_exists(table),
    )


@bp.get("/<username>/<table_name:table_name>/settings")
def table_settings(username: str, table_name: str) -> str:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    am_user_or_400(username)

    table_readme_markdown = svc.get_readme_markdown(sesh, table.table_uuid)

    return render_template(
        "table_settings.html",
        username=username,
        page_title=f"Settings: {username}/{table_name}",
        table_readme=table_readme_markdown or "",
        DataLicence=DataLicence,
        table=table,
        praise_id=get_praise_id_if_exists(table),
    )


@bp.post("/<username>/<table_name:table_name>/settings")
def post_table_settings(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    caption = request.form["caption"]
    if "private" in request.form:
        is_public = False
    else:
        is_public = True
    data_licence = DataLicence(request.form.get("data-licence", type=int))

    readme_markdown = request.form.get("table-readme-markdown", "")
    svc.set_readme_markdown(sesh, g.current_user.user_uuid, table_name, readme_markdown)

    svc.update_table_metadata(sesh, table.table_uuid, is_public, caption, data_licence)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()

    flash(f"Saved settings for {username}/{table_name}")

    return redirect(
        url_for(
            "csvbase.table_settings",
            username=username,
            table_name=table_name,
        )
    )


@bp.post("/<username>/<table_name:table_name>/praise")
def praise_table(username: str, table_name: str) -> Response:
    whence = request.form["whence"]
    am_a_user_or_400()
    sesh = get_sesh()
    praise_id = request.form.get("praise-id", type=int, default=None)
    if praise_id:
        svc.unpraise(sesh, praise_id)
    else:
        svc.praise(sesh, username, table_name, g.current_user.user_uuid)
    sesh.commit()

    return redirect(whence)


@bp.post("/<username>/<table_name:table_name>/rows/")
@cross_origin(max_age=CORS_EXPIRY, methods=["POST"])
def create_row(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    table = svc.get_table(sesh, username, table_name)
    if not am_user(username):
        if am_a_user():
            raise exc.NotAllowedException()
        else:
            raise exc.NotAuthenticatedException()

    row: Row
    if request.mimetype == ContentType.JSON.value:
        row_as_dict = json_or_400()["row"]
        row = {c: row_as_dict[c.name] for c in table.user_columns()}
    elif request.mimetype == ContentType.HTML_FORM.value:
        row = {
            c: from_html_form_to_python(c.type_, request.form.get(c.name))
            for c in table.user_columns()
        }
    else:
        raise exc.WrongContentType(
            [ContentType.JSON, ContentType.HTML_FORM], request.mimetype
        )

    row_id = PGUserdataAdapter.insert_row(sesh, table.table_uuid, row)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()

    row[ROW_ID_COLUMN] = row_id

    content_type = negotiate_content_type(
        [ContentType.HTML, ContentType.JSON], ContentType.JSON
    )
    if content_type is ContentType.JSON:
        json_body = row_to_json_dict(table, row)
        response = jsonify(json_body)
        response.status_code = 201
        return response
    else:
        flash(f"Created row {row_id}")
        return redirect(
            url_for(
                "csvbase.table_view",
                username=username,
                table_name=table_name,
                n=row_id + 1,
                op="lt",
                highlight=row_id,
            )
        )


@bp.get("/<username>/<table_name:table_name>/rows/<int:row_id>")
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT", "DELETE"])
def get_row(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(username):
        raise exc.TableDoesNotExistException(username, table_name)
    table = svc.get_table(sesh, username, table_name)
    row = PGUserdataAdapter.get_row(sesh, table.table_uuid, row_id)
    if row is None:
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    if is_browser():
        return make_response(
            render_template(
                "row-view-or-edit.html",
                page_title=f"{username}/{table_name}/rows/{row_id}",
                row=row,
                row_id=row_id,
                table=table,
            )
        )
    else:
        table = svc.get_table(sesh, username, table_name)
        return jsonify(row_to_json_dict(table, row))


@bp.get("/<username>/<table_name:table_name>/add-row-form")
def row_add_form(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    if not table.is_public and not am_user(username):
        raise exc.TableDoesNotExistException(username, table_name)

    return make_response(
        render_template(
            "row-add.html",
            page_title=f"Add a row to {username}/{table_name}",
            table=table,
        )
    )


@bp.get("/<username>/<table_name:table_name>/rows/<int:row_id>/delete-check")
def row_delete_check(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(username):
        raise exc.TableDoesNotExistException(username, table_name)
    table = svc.get_table(sesh, username, table_name)
    row = PGUserdataAdapter.get_row(sesh, table.table_uuid, row_id)
    if row is None:
        raise exc.RowDoesNotExistException(username, table_name, row_id)

    return make_response(
        render_template(
            "row_delete_check.html",
            page_title=f"Delete {username}/{table_name}/rows/{row_id}?",
            row=row,
            row_id=row_id,
            table=table,
        )
    )


@bp.put("/<username>/<table_name:table_name>/rows/<int:row_id>")
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT", "DELETE"])
def update_row(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    body = json_or_400()
    if body["row_id"] != row_id:
        raise exc.InvalidRequest("can't change row ids via an update")
    row = {c: json_to_value(c.type_, body["row"][c.name]) for c in table.user_columns()}
    row[table.row_id_column()] = row_id

    if not PGUserdataAdapter.update_row(sesh, table.table_uuid, row_id, row):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()
    return jsonify(body)


@bp.delete("/<username>/<table_name:table_name>/rows/<int:row_id>")
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT", "DELETE"])
def delete_row(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)
    if not PGUserdataAdapter.delete_row(sesh, table.table_uuid, row_id):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()
    response = make_response()
    response.status_code = 204
    return response


@bp.post(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/delete-row-for-browsers"
)
def delete_row_for_browsers(username: str, table_name: str, row_id: int) -> Response:
    # extremely annoying to need a special endpoint for this but browser forms
    # don't support the DELETE verb
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)
    if not PGUserdataAdapter.delete_row(sesh, table.table_uuid, row_id):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()
    flash(f"Deleted row {row_id}")
    return redirect(
        url_for("csvbase.table_view", username=username, table_name=table_name)
    )


@bp.route(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/edit", methods=["POST"]
)
def update_row_by_form_post(username: str, table_name: str, row_id: int) -> Response:
    """The view that handles form submissions to edit rows."""
    # after editing, we return them back to where they came from (or a table
    # page with this row on it)
    # FIXME: need some better, more consistent way to get whence
    whence = request.args.get(
        "whence",
        url_for(
            "csvbase.table_view",
            username=username,
            table_name=table_name,
            n=row_id + 1,
            op="lt",
        ),
    )

    # if they came from the table view page for this table and are being sent
    # back there via whence, then set this row as the highlight
    whence_view_func_and_args = reverse_url_for(whence)
    if whence_view_func_and_args is not None:
        vf = whence_view_func_and_args[0]
        vf_username = whence_view_func_and_args[1].get("username")
        vf_table_name = whence_view_func_and_args[1].get("table_name")
        if (
            hasattr(vf, "view_class")
            and vf.view_class == TableView
            and vf_username == username
            and vf_table_name == table_name
        ):
            s, n, p, q, f = urlsplit(whence)
            query_md: OrderedMultiDict[str, str] = OrderedMultiDict(parse_qsl(q))  # type: ignore
            query_md.setlist("highlight", [str(row_id)])
            whence = urlunsplit((s, n, p, urlencode(query_md), f))

    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    row: Row = {
        c: from_html_form_to_python(c.type_, request.form.get(c.name))
        for c in table.columns
    }
    PGUserdataAdapter.update_row(sesh, table.table_uuid, row_id, row)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()
    flash(f"Updated row {row_id}")
    return redirect(whence)


@bp.get("/<username>")
def user(username: str) -> Response:
    sesh = get_sesh()
    include_private = False
    if "current_user" in g and g.current_user.username == username:
        include_private = True
        has_subscription = billing_svc.has_subscription(sesh, g.current_user.user_uuid)
    else:
        has_subscription = False
    user = svc.user_by_name(sesh, username)
    tables = svc.tables_for_user(
        sesh,
        user.user_uuid,
        include_private=include_private,
    )
    return make_response(
        render_template(
            "user.html",
            user=user,
            page_title=f"{username}",
            tables=list(tables),
            show_manage_subscription=has_subscription,
        )
    )


@bp.route("/<username>/settings", methods=["GET", "POST"])
def user_settings(username: str) -> Response:
    am_user_or_400(username)
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    timezones = sorted(get_zonefile_instance().zones)
    if request.method == "GET":
        return make_response(
            render_template(
                "user-settings.html",
                user=user,
                page_title=f"{username} settings",
                timezones=timezones,
            )
        )
    else:
        timezone = request.form["timezone"]
        if timezone not in timezones:
            raise exc.InvalidRequest()
        user.timezone = timezone
        user.email = request.form["email"]
        svc.update_user(sesh, user)
        sesh.commit()
        flash("Updated settings")
        return redirect(url_for("csvbase.user_settings", username=username))


@bp.get("/robots.txt")
def robots() -> Response:
    sitemap_url = url_for("csvbase.sitemap", _external=True)
    robots_doc = f"Sitemap: {sitemap_url}"
    resp = make_response(robots_doc)
    resp.cache_control.public = True
    resp.cache_control.max_age = int(timedelta(days=1).total_seconds())
    return resp


@bp.get("/sitemap.xml")
def sitemap() -> Response:
    sesh = get_sesh()
    table_names = svc.get_public_table_names(sesh)
    # FIXME: include blog urls
    table_urls = (
        (
            url_for(
                "csvbase.table_view",
                username=username,
                table_name=table_name,
                _external=True,
            ),
            last_changed,
        )
        for username, table_name, last_changed in table_names
    )
    resp = make_response(render_template("sitemap.xml", urls=table_urls))
    resp.mimetype = "application/xml"
    resp.cache_control.public = True
    resp.cache_control.max_age = int(timedelta(days=1).total_seconds())
    return resp


@bp.route("/register", methods=["GET", "POST"])
def register() -> Response:
    if request.method == "GET":
        current_user = get_current_user()
        if current_user is not None:
            flash("You're already registered!")
            return redirect(url_for("csvbase.user", username=current_user.username))

        response = make_response(
            render_template(
                "register.html", whence=request.referrer, page_title="Register"
            )
        )
        return response
    else:
        sesh = get_sesh()
        username = request.form["username"]

        user = svc.create_user(
            sesh,
            current_app.config["CRYPT_CONTEXT"],
            username,
            request.form["password"],
            request.form.get("email"),
        )
        sesh.commit()
        sign_in_user(user)
        flash("Account registered")
        whence = request.form.get(
            "whence", url_for("csvbase.user", username=user.username)
        )
        return redirect(whence)


@bp.route("/sign-in", methods=["GET", "POST"])
def sign_in() -> Response:
    if request.method == "GET":
        response = make_response(
            render_template(
                "sign_in.html", whence=request.referrer, page_title="Sign in"
            )
        )
        return response
    else:
        sesh = get_sesh()
        username = request.form["username"]
        if svc.is_correct_password(
            sesh,
            current_app.config["CRYPT_CONTEXT"],
            username,
            request.form["password"],
        ):
            sign_in_user(
                svc.user_by_name(sesh, request.form["username"]),
            )
            flash(f"Signed in as {username}")
            if "whence" in request.form:
                return redirect(request.form["whence"])
            else:
                return redirect(
                    url_for("csvbase.user", username=request.form["username"])
                )
        else:
            logger.warning("wrong password for %s", username)
            raise exc.WrongAuthException()


@bp.get("/sign-out")
def sign_out():
    flask_session.clear()
    flash("Signed out")
    if request.referrer:
        return redirect(request.referrer)
    else:
        return redirect(url_for("csvbase.paste"))


def am_user(username: str) -> bool:
    """Return true if the current user has the given username."""
    current_user = g.get("current_user", None)
    if current_user is None or current_user.username != username:
        return False
    else:
        return True


def am_a_user() -> bool:
    return "current_user" in g


def am_user_or_400(username: str) -> bool:
    if not am_user(username):
        raise exc.NotAuthenticatedException()
    return True


def am_a_user_or_400():
    if not am_a_user():
        raise exc.NotAuthenticatedException()


def make_download_filename(username: str, table_name: str, extension: str) -> str:
    timestamp = date.today().isoformat()
    return f"{table_name}-{timestamp}.{extension}"


def make_streaming_response(
    response_buf: io.BytesIO,
    content_type: Optional[ContentType] = None,
    download_filename: Optional[str] = None,
) -> Response:
    """Turn a stream into an streaming flask response."""

    def generate() -> Union[Iterator[bytes], Iterator[str]]:
        minibuf = response_buf.read(COPY_BUFFER_SIZE)
        while minibuf:
            yield minibuf
            minibuf = response_buf.read(COPY_BUFFER_SIZE)

    mimetype: str = (
        "application/octet-stream" if content_type is None else content_type.value
    )
    response = current_app.response_class(generate(), mimetype=mimetype)

    # Setting Content-Length is optional but helps clients allocate buffers
    response.headers["Content-Length"] = str(streams.file_length(response_buf))

    if download_filename is not None:
        response.headers["Content-Disposition"] = (
            f'attachment; filename="{download_filename}"'
        )
    return response


def negotiate_content_type(
    supported_mediatypes: Sequence[ContentType], default: Optional[ContentType] = None
) -> ContentType:
    accepts = werkzeug.http.parse_accept_header(request.headers.get("Accept"))
    best = accepts.best_match(
        [ct.value for ct in supported_mediatypes],
        default=default.value if default is not None else None,
    )
    if best is None:
        raise exc.CantNegotiateContentType(supported_mediatypes)
    else:
        return ContentType(best)


def sign_in_user(user: User, session: Optional[Any] = None) -> None:
    """Sets the current user and sets a cookie to keep them logged in across
    requests.

    """
    set_current_user(user)

    if session is None:
        session = flask_session
    session["user_uuid"] = user.user_uuid
    # Make it last for 31 days
    session.permanent = True


def register_and_sign_in_new_user(sesh, form: ImmutableMultiDict[str, str]) -> User:
    """Registers a new user and signs them in if the registration succeeds."""
    new_user = svc.create_user(
        sesh,
        current_app.config["CRYPT_CONTEXT"],
        form["username"],
        form["password"],
        form.get("email"),
    )
    sign_in_user(new_user)
    flash("Account registered")
    return new_user


def json_or_400() -> Dict[str, Any]:
    if request.json is None:
        raise exc.InvalidRequest()
    else:
        return request.json


def keyset_from_request_args() -> KeySet:
    n: int = request.args.get("n", default=0, type=int)
    op: Literal["greater_than", "less_than"] = (
        "greater_than" if request.args.get("op", default="gt") == "gt" else "less_than"
    )
    keyset = KeySet([Column("csvbase_row_id", ColumnType.INTEGER)], (n,), op=op)
    return keyset


def row_to_json_dict(table: Table, row: Row, omit_row_id=False) -> Dict[str, Any]:
    # FIXME: rename "omit_row_id" to "as_though_unsubmitted" or something
    row_without_row_id = (c for c in row.items() if c[0].name != "csvbase_row_id")
    json_dict: Dict = {
        "row": {
            column.name: value_to_json(value) for column, value in row_without_row_id
        },
    }
    if not omit_row_id:
        row_id = row_id_from_row(row)
        json_dict["row_id"] = row_id
        json_dict["url"] = url_for(
            "csvbase.get_row",
            username=table.username,
            table_name=table.table_name,
            row_id=row_id,
            _external=True,
        )

    return json_dict


def row_id_from_row(row: Row) -> int:
    return cast(int, row[ROW_ID_COLUMN])


def page_to_json_dict(table: Table, page: Page) -> Dict[str, Any]:
    rv: Dict[str, Any] = {}
    rv["rows"] = [row_to_json_dict(table, row) for row in page.rows]
    if page.has_less:
        # FIXME: these url_fors should be shared with the table_view template
        rv["previous_page_url"] = url_for(
            "csvbase.table_view",
            username=table.username,
            table_name=table.table_name,
            op="lt",
            n=page.rows[-1][ROW_ID_COLUMN],
            _external=True,
        )
    else:
        rv["previous_page_url"] = None

    if page.has_more:
        rv["next_page_url"] = url_for(
            "csvbase.table_view",
            username=table.username,
            table_name=table.table_name,
            op="gt",
            n=page.rows[-1][ROW_ID_COLUMN],
            _external=True,
        )
    else:
        rv["next_page_url"] = None
    return rv


def table_to_json_dict(table: Table, page: Page) -> Dict[str, Any]:
    rv = {
        "name": table.table_name,
        "is_public": table.is_public,
        "caption": table.caption,
        "data_licence": table.data_licence.short_render(),
        "created": table.created.isoformat(),
        "last_changed": table.last_changed.isoformat(),
        "columns": [
            {"name": column.name, "type": column.type_.pretty_type()}
            for column in table.columns
        ],
        "page": page_to_json_dict(table, page),
        "approx_size": table.row_count.best(),
    }
    return rv


def url_for_with_auth(endpoint: str, **values) -> str:
    """Build a url, but add basic auth (if the users is logged in)"""
    flask_url = url_for(endpoint, **values)
    current_user = get_current_user()
    if current_user is not None:
        username = g.current_user.username
        password = g.current_user.hex_api_key()
    else:
        # if they aren't signed in, just use placeholder strings
        username = "<some_user>"
        password = "<some_api_key>"  # nosec B105
    s, n, p, q, f = urlsplit(flask_url)
    authed_netloc = f"{username}:{password}@{n}"
    final_url = urlunsplit((s, authed_netloc, p, q, f))
    return final_url


def get_praise_id_if_exists(table: Table) -> Optional[int]:
    sesh = get_sesh()
    if "current_user" in g:
        return svc.is_praised(sesh, g.current_user.user_uuid, table.table_uuid)
    else:
        return None


def from_html_form_to_python(
    column_type: ColumnType, form_value: Optional[str]
) -> Optional["PythonType"]:
    """Parses values from HTML forms into Python objects, according to ColumnType."""
    if column_type is ColumnType.BOOLEAN:
        return True if form_value == "on" else False
    elif column_type is ColumnType.DATE:
        return DateConverter().convert(form_value or "")
    elif column_type is ColumnType.INTEGER:
        return IntegerConverter().convert(form_value or "")
    elif column_type is ColumnType.FLOAT:
        return FloatConverter().convert(form_value or "")
    elif form_value in {None, ""}:
        # FIXME: This is slightly odd as it returns "" when the user might mean
        # null, but that needs a bigger fix, see:
        # https://github.com/calpaterson/csvbase/issues/15
        return ""
    else:
        return column_type.python_type()(form_value)


def readme_html(sesh, table_uuid: UUID) -> Optional[str]:
    readme_markdown = svc.get_readme_markdown(sesh, table_uuid)
    if readme_markdown is not None:
        readme_html = render_markdown(readme_markdown)
        return readme_html
    else:
        return None
