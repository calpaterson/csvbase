import io
from uuid import UUID
from pathlib import Path
import shutil
from datetime import timedelta, timezone
import codecs
from logging import getLogger
from typing import (
    Any,
    Dict,
    Mapping,
    Optional,
    Sequence,
    Union,
    cast,
    Iterator,
    IO,
    TypeVar,
    List,
)
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode
import hashlib
import json
import dataclasses

import pydantic
from sqlalchemy.orm import Session
from dateutil.zoneinfo import get_zonefile_instance
import itsdangerous.url_safe
from werkzeug.datastructures import OrderedMultiDict
import werkzeug.http
from flask import (
    Blueprint,
    current_app,
    flash,
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

from ..func import (
    get_current_user_or_401,
    get_current_user,
    reverse_url_for,
    safe_redirect,
    sign_in_user,
    register_and_sign_in_new_user,
    ensure_table_access,
    am_user,
    am_a_user,
    am_user_or_400,
    ensure_not_read_only,
)
from ... import exc, svc, streams, table_io
from ...json import value_to_json, json_to_row
from ...markdown import render_markdown
from ...sesh import get_sesh
from ...userdata import PGUserdataAdapter
from ...conv import DateConverter, IntegerConverter, FloatConverter
from ...value_objs import (
    ROW_ID_COLUMN,
    Column,
    ColumnType,
    ContentType,
    DataLicence,
    KeySet,
    Page,
    PythonType,
    Row,
    Table,
    Backend,
)
from ...constants import COPY_BUFFER_SIZE
from ..billing import svc as billing_svc
from ...repcache import RepCache
from ...config import get_config
from csvbase.bgwork import task_registry

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


@bp.get("/")
def index() -> str:
    sesh = get_sesh()
    return render_template("index.html", top_ten=svc.get_top_n(sesh))


@bp.get("/about")
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


class TableView(MethodView):
    """This covers the "table API" plus the HTML "View" page."""

    def head(self, username: str, table_name: str) -> Response:
        """Get (HTTP-level) metadata about a table.

        Flask auto-implements this by doing a GET and stripping the body, but
        this implementation avoids that work.
        """
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "read")

        response = Response(status=200)

        # No ETag is set because a HEAD request is not about a specific
        # representation and we don't know what ETag to set.  We're simply
        # saying that the table exists.
        response = add_table_view_cache_headers(table, response, etag=None)
        response = add_table_metadata_headers(table, response)

        return response

    def get(self, username: str, table_name: str) -> Response:
        """Get a table"""
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "read")
        content_type = negotiate_content_type(
            [
                ContentType.CSV,
                ContentType.HTML,
                ContentType.JSON,
                ContentType.PARQUET,
                ContentType.XLSX,
                ContentType.JSON_LINES,
            ]
        )
        return make_table_view_response(sesh, content_type, table)

    def put(self, username: str, table_name: str) -> Response:
        """Create or overwrite a table."""
        sesh = get_sesh()
        user = svc.user_by_name(sesh, username)
        negotiate_content_type([ContentType.JSON])

        backend = PGUserdataAdapter(sesh)

        if svc.table_exists(sesh, user.user_uuid, table_name):
            table = svc.get_table(sesh, username, table_name)
            ensure_table_access(sesh, table, "write")
            ensure_not_read_only(table)

            create_only = request.headers.get("If-None-Match", None) == "*"
            if create_only:
                raise exc.TableAlreadyExists()

            provided_etag = request.headers.get("If-Weak-Match", None)
            if provided_etag is not None:
                keyset = KeySet(
                    [Column("csvbase_row_id", ColumnType.INTEGER)],
                    (0,),
                    op="greater_than",
                )
                expected_etag = make_table_view_etag(table, ContentType.CSV, keyset)
                if provided_etag != expected_etag:
                    raise exc.ETagMismatch()

            str_buf = get_user_str_buf()
            dialect, csv_columns = streams.peek_csv(str_buf, table.columns)
            rows = table_io.csv_to_rows(str_buf, csv_columns, dialect)

            # If there is no csvbase_row_id column, don't try to correlate
            # updates, just wipe the table and insert everything.
            if "csvbase_row_id" not in set(c.name for c in csv_columns):
                backend.delete_table_data(table)
                backend.insert_table_data(table, csv_columns, rows)
            else:
                backend.upsert_table_data(
                    table,
                    csv_columns,
                    rows,
                )
            status = 200
            message = f"upserted {username}/{table_name}"
        else:
            # FIXME: this logic to check if the user *could* write to this
            # table, if it existed, should be encapsulated somewhere else and
            # re-used here
            if not am_user(username):
                is_public = svc.is_public(sesh, username, table_name)
                if is_public and am_a_user():
                    raise exc.NotAllowedException()
                elif is_public:
                    raise exc.NotAuthenticatedException()
                else:
                    raise exc.TableDoesNotExistException(username, table_name)

            str_buf = get_user_str_buf()
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
                Backend.POSTGRES,
            )
            backend.create_table(table_uuid, csv_columns)
            table = svc.get_table(sesh, username, table_name)
            backend.insert_table_data(table, csv_columns, rows)
            status = 201
            message = f"created {username}/{table_name}"
        svc.update_upstream(sesh, table)
        svc.mark_table_changed(sesh, table.table_uuid)
        sesh.commit()
        response = jsonify({"message": message})
        response.status_code = status
        return response

    def delete(self, username: str, table_name: str) -> Response:
        """Delete a table"""
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "write")
        svc.delete_table_and_metadata(sesh, username, table_name)
        sesh.commit()

        message = f"deleted {username}/{table_name}"
        response = jsonify({"message": message})
        response.status_code = 204
        return response

    def post(self, username: str, table_name: str) -> Response:
        """Append some new rows to a table."""
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "write")
        ensure_not_read_only(table)

        negotiate_content_type([ContentType.JSON])

        str_buf = get_user_str_buf()
        dialect, columns = streams.peek_csv(str_buf, table.columns)
        rows = table_io.csv_to_rows(str_buf, columns, dialect)

        # FIXME: check that columns is a subset of table_columns
        # table_columns = PGUserdataAdapter.get_columns(sesh, columns)

        backend = PGUserdataAdapter(sesh)
        backend.insert_table_data(table, columns, rows)

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
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")
    svc.delete_table_and_metadata(sesh, username, table_name)
    sesh.commit()
    flash(f"Deleted {username}/{table_name}")
    return redirect(url_for("csvbase.user", username=username))


@bp.get("/<username>/<table_name:table_name>/export/<extension>")
@bp.get("/<username>/<table_name:table_name>.<extension>")
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT"])
def table_view_with_extension(
    username: str, table_name: str, extension: str
) -> Response:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "read")

    content_type = ContentType.from_file_extension(extension)
    if content_type is None:
        raise exc.CantNegotiateContentType([e for e in ContentType])

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
        response = add_table_view_cache_headers(table, response, etag)
        response = add_table_metadata_headers(table, response)
        return response
    elif if_none_match is not None:
        logger.info(
            "provided etag (%s) doesn't match current (%s)", if_none_match, etag
        )
    else:
        logger.debug("no matching etag")

    backend = PGUserdataAdapter(sesh)
    # If the representation is page based:
    if content_type in {ContentType.HTML, ContentType.JSON}:
        page = backend.table_page(table, keyset)
        ensure_not_over_the_top(table, keyset, page)
        if content_type is ContentType.HTML:
            min_row_id, max_row_id = backend.row_id_bounds(table.table_uuid)
            row_ids = page.row_ids()
            is_first_page = min_row_id is None or (min_row_id in row_ids)
            is_last_page = max_row_id is None or (max_row_id in row_ids)

            reps = get_table_reps(sesh, table)

            template_kwargs = dict(
                page_title=f"{table.username}/{table.table_name}",
                table=table,
                page=page,
                keyset=keyset,
                ROW_ID_COLUMN=ROW_ID_COLUMN,
                praise_id=get_praise_id_if_exists(sesh, table),
                is_first_page=is_first_page,
                is_last_page=is_last_page,
                max_row_id=max_row_id,
                highlight=request.args.get("highlight", None, type=int),
                reps=reps,
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
            response = add_table_view_cache_headers(table, response)
            response = add_table_metadata_headers(table, response)
            return response
        else:
            response = add_table_view_cache_headers(
                table, jsonify(table_to_json_dict(table, page)), etag
            )
            response = add_table_metadata_headers(table, response)
            return response
    else:
        download_filename = make_download_filename(
            table.username, table.table_name, content_type.file_extension()
        )

        repcache = RepCache(table.table_uuid, content_type, table.last_changed)
        if not repcache.exists():
            is_big = backend.count(table.table_uuid).is_big()
            if is_big:
                if not repcache.write_in_progress():
                    task_registry.populate_repcache.delay(
                        table.table_uuid, content_type.value
                    )
                response = make_wait_response(table, content_type)
                return response

            svc.populate_repcache(sesh, table.table_uuid, content_type)

        if get_config().x_accel_redirect:
            response = make_response()
            repcache_path = repcache.path()
            response.headers["X-Accel-Redirect"] = f"/repcache/{repcache_path}"
        else:
            with repcache.open(mode="rb") as response_buf:
                response = make_streaming_response(
                    response_buf, content_type, download_filename
                )
        with_cache_headers = add_table_view_cache_headers(table, response, etag)
        with_metadata_headers = add_table_metadata_headers(table, with_cache_headers)
        return with_metadata_headers


def make_wait_response(table: Table, content_type: ContentType) -> Response:
    wait_content_type = negotiate_content_type([ContentType.HTML])
    delay_seconds = 10
    if wait_content_type is ContentType.HTML:
        response = make_response(
            render_template(
                "table_wait.html",
                page_title="Please wait...",
                table=table,
                content_type=content_type,
                delay_seconds=delay_seconds,
            ),
            503,
        )

    response.headers["Retry-After"] = str(delay_seconds)
    response.headers["Refresh"] = str(delay_seconds)
    return response


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
    """Returns the ETag for a given (table, content_type, keyset)."""
    current_user = get_current_user()
    current_username = (
        current_user.username if current_user is not None else "anonymous"
    )
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


def make_row_etag(table: Table, row: Row, content_type: ContentType) -> str:
    """Returns the ETag for the given Row."""
    current_user = get_current_user()
    current_username = (
        current_user.username if current_user is not None else "anonymous"
    )
    hash_ = hashlib.blake2b()
    hash_.update(json.dumps(row_to_json_dict(table, row)).encode("utf-8"))
    hash_.update(content_type.value.encode("utf-8"))
    if content_type is ContentType.HTML:
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
    response.headers["Link"] = f'<{ url }>; rel="canonical"'
    # setting the 'Last-Modified' seems to make varnish get it badly wrong, so
    # set this for now
    response.headers["CSVBase-Last-Modified"] = last_changed.strftime(
        "%a, %d %b %Y %H:%M:%S GMT"
    )
    return response


def add_table_view_cache_headers(
    table: Table, response: Response, etag: Optional[str] = None
) -> Response:
    """Set the Cache-Control, ETag and xkey (varnish) cache headers relevant to
    table views."""
    if etag is not None:
        response.headers["ETag"] = etag

    # Confusingly, no-cache indicates that caches may cache, but must
    # revalidate the respresention each time (eg with ETags)
    response.cache_control.no_cache = True

    # It seems that caches are allowed to ignore `no-cache` in "exceptional"
    # circumstances.  This header ensures that they do not.
    response.cache_control.must_revalidate = True

    # The advice on "Vary" is to return the same value for every response from
    # a URL, and we want the cache key to include "Cookie"
    response.headers["Vary"] = "Accept, Cookie"

    # HTML views show usernames, other personal data.  "private" restricts
    # caching of these responses to local caches only.  This is also set for
    # private tables.
    if response.mimetype == ContentType.HTML.value or not table.is_public:
        response.cache_control.private = True

    # xkeys are used by varnish to do invalidation - currently not used because
    # etags works well enough for now
    # response.headers["xkey"] = "table/{table_uuid}"
    return response


def add_row_view_cache_headers(
    table: Table, row: Row, response: Response, etag: Optional[str] = None
) -> Response:
    if etag is not None:
        response.headers["ETag"] = etag

    # see comments in add_table_view_cache_headers
    # FIXME: probably, this code should be shared
    response.cache_control.no_cache = True
    response.cache_control.must_revalidate = True
    response.headers["Vary"] = "Accept, Cookie"
    if response.mimetype == ContentType.HTML.value or not table.is_public:
        response.cache_control.private = True

    return response


@bp.get("/<username>/<table_name:table_name>/readme")
def table_readme(username: str, table_name: str) -> Response:
    sesh = get_sesh()

    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "read")

    readme_markdown = svc.get_readme_markdown(sesh, table.table_uuid)

    if readme_markdown is not None:
        readme_html = render_markdown(readme_markdown)
    else:
        readme_html = "(no readme set)"

    reps = get_table_reps(sesh, table)

    return make_response(
        render_template(
            "table_readme.html",
            page_title=f"Readme for {username}/{table_name}",
            table=table,
            table_readme=readme_html,
            praise_id=get_praise_id_if_exists(sesh, table),
            reps=reps,
        )
    )


@bp.get("/<username>/<table_name:table_name>/docs")
def get_table_apidocs(username: str, table_name: str) -> str:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "read")
    owner = svc.user_by_name(sesh, username)

    made_up_row = svc.get_a_made_up_row(sesh, table.table_uuid)
    backend = PGUserdataAdapter(sesh)
    sample_row = backend.get_a_sample_row(table.table_uuid)
    sample_page = Page(has_less=False, has_more=True, rows=[sample_row])

    reps = get_table_reps(sesh, table)

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
        praise_id=get_praise_id_if_exists(sesh, table),
        reps=reps,
    )


class CopyView(MethodView):
    """For making copies of tables."""

    def get(self, username: str, table_name: str) -> Response:
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "read")
        response = make_response(
            render_template(
                "copy.html", table=table, page_title=f"Copy {username}/{table_name}"
            )
        )
        return response

    def post(self, username: str, table_name: str) -> Response:
        sesh = get_sesh()

        if "username" in request.form:
            current_user = register_and_sign_in_new_user(sesh)
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
        ensure_table_access(sesh, existing_table, "read")
        new_table_name = request.form["table-name"]

        is_public = not private
        new_table_uuid = svc.create_table_metadata(
            sesh,
            current_user.user_uuid,
            new_table_name,
            is_public,
            existing_table.caption,
            existing_table.data_licence,
            Backend.POSTGRES,
        )

        backend = PGUserdataAdapter(sesh)
        backend.create_table(new_table_uuid, existing_table.columns)
        backend.copy_table_data(existing_table.table_uuid, new_table_uuid)
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
    ensure_table_access(sesh, table, "read")
    reps = get_table_reps(sesh, table)

    return render_template(
        "table_details.html",
        username=username,
        page_title=f"Schema & Details: {username}/{table_name}",
        DataLicence=DataLicence,
        table=table,
        praise_id=get_praise_id_if_exists(sesh, table),
        reps=reps,
    )


@bp.get("/<username>/<table_name:table_name>/settings")
def table_settings(username: str, table_name: str) -> str:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")

    table_readme_markdown = svc.get_readme_markdown(sesh, table.table_uuid)

    reps = get_table_reps(sesh, table)

    return render_template(
        "table_settings.html",
        username=username,
        page_title=f"Settings: {username}/{table_name}",
        table_readme=table_readme_markdown or "",
        DataLicence=DataLicence,
        table=table,
        praise_id=get_praise_id_if_exists(sesh, table),
        reps=reps,
    )


@bp.post("/<username>/<table_name:table_name>/settings")
def post_table_settings(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")

    caption = request.form["caption"]
    if "private" in request.form:
        is_public = False
    else:
        is_public = True
    data_licence = DataLicence(request.form.get("data-licence", type=int))

    readme_markdown = request.form.get("table-readme-markdown", "")
    current_user = get_current_user_or_401()
    svc.set_readme_markdown(sesh, current_user.user_uuid, table_name, readme_markdown)

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
    if not am_a_user():
        raise exc.NotAuthenticatedException()
    whence = get_whence(
        url_for("csvbase.table_view", username=username, table_name=table_name)
    )
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "read")
    praise_id = request.form.get("praise-id", type=int, default=None)
    current_user = get_current_user_or_401()
    if praise_id:
        svc.unpraise(sesh, praise_id)
    else:
        svc.praise(sesh, username, table_name, current_user.user_uuid)
    sesh.commit()

    return safe_redirect(whence)


@bp.get("/<username>/<table_name:table_name>/add-row-form")
def row_add_form(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")
    ensure_not_read_only(table)

    return make_response(
        render_template(
            "row-add.html",
            page_title=f"Add a row to {username}/{table_name}",
            table=table,
        )
    )


# FIXME: some more porting required in order to use this and have it raise
# TableDefinitionMismatchException
# def create_row_model(columns: Sequence[Column]) -> pydantic.BaseModel:
#     row_kwargs = {
#         c.name: (Optional[c.type_.python_type()], ...)
#         for c in columns
#     }
#     return pydantic.create_model("DynamicRowModel", **row_kwargs)


def create_row_body_model(columns: Sequence[Column], with_row_id: bool):
    title = "DynamicRowBodyModel"
    kwargs: Dict[str, Any] = {}
    if with_row_id:
        kwargs["row_id"] = (int, ...)
    # FIXME: should check this url
    kwargs["url"] = (Optional[str], None)
    # kwargs["row"] = (create_row_model(columns), ...)
    kwargs["row"] = (Dict[str, Any], ...)
    model = pydantic.create_model(title, __config__={"extra": "forbid"}, **kwargs)
    return model


@bp.post("/<username>/<table_name:table_name>/rows/")
@cross_origin(max_age=CORS_EXPIRY, methods=["POST"])
def create_row(username: str, table_name: str) -> Response:
    """Create a new row.  This handles both the API and the website's HTML forms."""
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")
    ensure_not_read_only(table)

    row: Row
    if request.mimetype == ContentType.JSON.value:
        json_body = json_or_400()
        try:
            create_row_body_model(table.columns, with_row_id=False)(**json_body)
        except pydantic.ValidationError:
            raise exc.InvalidRequest()
        row = json_to_row(table.user_columns(), json_body["row"])
    elif request.mimetype == ContentType.HTML_FORM.value:
        row = form_to_row(table.user_columns(), request.form)
    else:
        raise exc.WrongContentType(
            [ContentType.JSON, ContentType.HTML_FORM], request.mimetype
        )

    backend = PGUserdataAdapter(sesh)
    row_id = backend.insert_row(table.table_uuid, row)
    svc.mark_table_changed(sesh, table.table_uuid)
    svc.update_upstream(sesh, table)
    sesh.commit()

    row[ROW_ID_COLUMN] = row_id

    content_type = negotiate_content_type(
        [ContentType.JSON, ContentType.HTML],
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


class RowView(MethodView):
    """This covers the part of the row API that is concerned with named
    individual rows."""

    def get(self, username: str, table_name: str, row_id: int) -> Response:
        sesh = get_sesh()
        svc.user_exists(sesh, username)
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "read")
        backend = PGUserdataAdapter(sesh)
        row = backend.get_row(table.table_uuid, row_id)
        if row is None:
            raise exc.RowDoesNotExistException(username, table_name, row_id)

        content_type = negotiate_content_type([ContentType.JSON, ContentType.HTML])
        etag = make_row_etag(table, row, content_type)
        response: Response  # necessary to avoid inference as flask.wrappers.Response
        if content_type is ContentType.HTML:
            response = make_response(
                render_template(
                    "row-view-or-edit.html",
                    page_title=f"{username}/{table_name}/rows/{row_id}",
                    row=row,
                    row_id=row_id,
                    table=table,
                )
            )
        else:
            response = jsonify(row_to_json_dict(table, row))

        response = add_row_view_cache_headers(table, row, response, etag)
        return response

    def put(self, username: str, table_name: str, row_id: int) -> Response:
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "write")
        ensure_not_read_only(table)

        body = json_or_400()
        try:
            create_row_body_model(table.user_columns(), with_row_id=True)(**body)
        except pydantic.ValidationError:
            raise exc.InvalidRequest()
        if body["row_id"] != row_id:
            raise exc.InvalidRequest("can't change row ids via an update")
        row = json_to_row(table.user_columns(), body["row"])
        row[table.row_id_column()] = row_id

        backend = PGUserdataAdapter(sesh)
        if not backend.update_row(table.table_uuid, row_id, row):
            raise exc.RowDoesNotExistException(username, table_name, row_id)
        svc.update_upstream(sesh, table)
        svc.mark_table_changed(sesh, table.table_uuid)
        sesh.commit()
        return jsonify(body)

    def post(self, username: str, table_name: str, row_id: int) -> Response:
        """This endpoint is for HTML forms, which cannot do PUT but need a way
        to update rows.

        """
        # after editing, we return them back to where they came from (or a table
        # page with this row on it)
        whence = get_whence(
            url_for(
                "csvbase.table_view",
                username=username,
                table_name=table_name,
                n=row_id + 1,
                op="lt",
            )
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
        ensure_table_access(sesh, table, "write")
        ensure_not_read_only(table)
        if request.form.get("csvbase_row_id", None) is None:
            raise exc.InvalidRequest()
        row = form_to_row(table.columns, request.form)
        backend = PGUserdataAdapter(sesh)
        backend.update_row(table.table_uuid, row_id, row)
        svc.update_upstream(sesh, table)
        svc.mark_table_changed(sesh, table.table_uuid)
        sesh.commit()
        flash(f"Updated row {row_id}")
        return safe_redirect(whence)

    def delete(self, username: str, table_name: str, row_id: int) -> Response:
        sesh = get_sesh()
        table = svc.get_table(sesh, username, table_name)
        ensure_table_access(sesh, table, "write")
        backend = PGUserdataAdapter(sesh)
        if not backend.delete_row(table.table_uuid, row_id):
            raise exc.RowDoesNotExistException(username, table_name, row_id)
        svc.update_upstream(sesh, table)
        svc.mark_table_changed(sesh, table.table_uuid)
        sesh.commit()
        response = make_response()
        response.status_code = 204
        return response


bp.add_url_rule(
    "/<username>/<table_name:table_name>/rows/<int:row_id>",
    view_func=RowView.as_view("row_view"),
)


@bp.get("/<username>/<table_name:table_name>/rows/<int:row_id>/delete-check")
def row_delete_check(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")
    ensure_not_read_only(table)
    backend = PGUserdataAdapter(sesh)
    row = backend.get_row(table.table_uuid, row_id)
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


@bp.post(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/delete-row-for-browsers"
)
def delete_row_for_browsers(username: str, table_name: str, row_id: int) -> Response:
    # extremely annoying to need a special endpoint for this but browser forms
    # don't support the DELETE verb
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    ensure_table_access(sesh, table, "write")
    ensure_not_read_only(table)
    backend = PGUserdataAdapter(sesh)
    if not backend.delete_row(table.table_uuid, row_id):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    svc.update_upstream(sesh, table)
    svc.mark_table_changed(sesh, table.table_uuid)
    sesh.commit()
    flash(f"Deleted row {row_id}")
    return redirect(
        url_for("csvbase.table_view", username=username, table_name=table_name)
    )


@bp.get("/<username>")
def user(username: str) -> Response:
    sesh = get_sesh()
    include_private = False
    current_user = get_current_user()
    if current_user is not None and current_user.username == username:
        include_private = True
        has_subscription = billing_svc.has_subscription(sesh, current_user.user_uuid)
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
        user.email = request.form.get("email")
        user.mailing_list = request.form.get("mailing-list", type=bool, default=False)
        svc.update_user(sesh, user)
        sesh.commit()
        flash("Updated settings")
        return redirect(url_for("csvbase.user_settings", username=username))


@bp.route("/<username>/settings/change-password", methods=["GET", "POST"])
def change_password(username: str) -> Response:
    am_user_or_400(username)
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    if request.method == "GET":
        return make_response(
            render_template(
                "change-password.html",
                page_title="Change password",
            )
        )
    else:
        crypt_context = current_app.config["CRYPT_CONTEXT"]
        existing_password = request.form["existing-password"]
        new_password = request.form["new-password"]
        new_password_again = request.form["new-password-again"]
        if not svc.is_correct_password(
            sesh, crypt_context, user.username, existing_password
        ):
            logger.warning(
                "user '%s' tried to change password but got password wrong",
                user.username,
            )
            flash("Existing password not correct")
            return make_response(
                render_template(
                    "change-password.html",
                    page_title="Change password",
                ),
                400,
            )
        if new_password != new_password_again:
            logger.warning(
                "user '%s' tried to change password but mistyped confirmation", username
            )
            flash("Passwords don't match")
            return make_response(
                render_template(
                    "change-password.html",
                    page_title="Change password",
                ),
                400,
            )
        svc.set_password(sesh, crypt_context, user.user_uuid, new_password)
        sesh.commit()
        flash("Password changed")
        return redirect(url_for("csvbase.user", username=username))


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
        user = register_and_sign_in_new_user(sesh)
        # FIXME: why doesn't commenting this out cause tests to fail
        # https://github.com/calpaterson/csvbase/issues/117
        sesh.commit()
        whence = get_whence(url_for("csvbase.user", username=user.username))
        return safe_redirect(whence)


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
            whence = get_whence(
                url_for("csvbase.user", username=request.form["username"])
            )
            return safe_redirect(whence)
        else:
            logger.warning("wrong password for %s", username)
            raise exc.WrongAuthException()


@bp.get("/sign-out")
def sign_out():
    flask_session.clear()
    flash("Signed out")
    if request.referrer:
        response = safe_redirect(request.referrer)
    else:
        response = redirect(url_for("csvbase.index"))

    # session is already cleared, this is just to make doubly sure
    response.headers["Clear-Site-Data"] = "*"

    return response


def make_download_filename(username: str, table_name: str, extension: str) -> str:
    return f"{table_name}.{extension}"


def make_streaming_response(
    response_buf: IO[bytes],
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


def negotiate_content_type(supported_mediatypes: Sequence[ContentType]) -> ContentType:
    """Negotiate the format to send back to the client."""
    accepts = werkzeug.http.parse_accept_header(request.headers.get("Accept", "*/*"))
    best = accepts.best_match(
        [ct.value for ct in supported_mediatypes],
    )
    if best is None:
        # werkzeug doesn't handle */*
        if "*/*" in accepts:
            return supported_mediatypes[0]
        raise exc.CantNegotiateContentType(supported_mediatypes)
    else:
        return ContentType(best)


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
            "csvbase.row_view",
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
        username = current_user.username
        password = current_user.hex_api_key()
    else:
        # if they aren't signed in, just use placeholder strings
        username = "<some_user>"
        password = "<some_api_key>"  # nosec B105
    s, n, p, q, f = urlsplit(flask_url)
    authed_netloc = f"{username}:{password}@{n}"
    final_url = urlunsplit((s, authed_netloc, p, q, f))
    return final_url


def get_praise_id_if_exists(sesh: Session, table: Table) -> Optional[int]:
    current_user = get_current_user()
    if current_user is not None:
        return svc.is_praised(sesh, current_user.user_uuid, table.table_uuid)
    else:
        return None


def form_to_row(
    columns: Sequence[Column], form: Mapping[str, str]
) -> Dict[Column, Optional[PythonType]]:
    row = {}
    in_table = set(c.name for c in columns)
    present = set(form.keys())
    extra = present.difference(in_table)
    if len(extra) > 0:
        raise exc.TableDefinitionMismatchException()
    for column in columns:
        if column.name in form:
            row[column] = from_html_form_to_python(column.type_, form[column.name])
        else:
            row[column] = None
    return row


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


W = TypeVar("W", bound=Optional[str])


def get_whence(default: W) -> Union[str, W]:
    """ "Divine where the user came from, so that they can be sent back."""
    from_args = request.args.get("whence", default)
    if from_args is not None:
        return from_args
    else:
        return request.form.get("whence", default)


def get_user_str_buf() -> codecs.StreamReader:
    """Return the streamed request data the user supplied, as a a StringIO."""
    byte_buf = io.BytesIO()
    with streams.rewind(byte_buf):
        shutil.copyfileobj(request.stream, byte_buf)
    str_buf = streams.byte_buf_to_str_buf(byte_buf)
    return str_buf


@dataclasses.dataclass
class TableRepresentation:
    """Convenience object holding metadata on a specific representation (eg:
    csv) of a table."""

    content_type: ContentType
    offered: bool
    size: int
    size_is_estimate: bool


def get_table_reps(sesh: Session, table: Table) -> List[TableRepresentation]:
    supported_content_types = [
        ContentType.CSV,
        ContentType.PARQUET,
        ContentType.XLSX,
        ContentType.JSON_LINES,
    ]

    rep_sizes = RepCache.sizes(table.table_uuid, table.last_changed)
    backend = PGUserdataAdapter(sesh)
    is_big = backend.count(table.table_uuid).is_big()

    rv = []

    for content_type in supported_content_types:
        if is_big and content_type is ContentType.XLSX:
            offered = False
        else:
            offered = True
        if content_type in rep_sizes:
            size = rep_sizes[content_type]
            size_is_estimate = False
        else:
            size = 0
            size_is_estimate = True
        rv.append(
            TableRepresentation(
                content_type=content_type,
                offered=offered,
                size=size,
                size_is_estimate=size_is_estimate,
            )
        )
    return rv
