"""Machinery for the creation of new tables."""

from base64 import b64encode, b64decode
import json
import zlib
import io
from logging import getLogger
from typing import List, Tuple, Dict, Mapping
import secrets
from urllib.parse import urlparse, ParseResult

import giturlparse
from flask.views import MethodView
from flask import Blueprint, redirect, render_template, url_for, request
from werkzeug.wrappers.response import Response

from ..func import (
    get_current_user_or_401,
    licence_form_field_to_licence,
    ORDERED_LICENCES,
)
from ... import exc, svc, streams, table_io, temp
from ...sesh import get_sesh
from ...userdata import PGUserdataAdapter
from ...follow.git import GitSource
from ...value_objs import (
    Column,
    ColumnType,
    Encoding,
    Backend,
    GitUpstream,
)
from ...streams import UserSubmittedCSVData
from ..billing import svc as billing_svc

bp = Blueprint("create_table", __name__)

logger = getLogger(__name__)


@bp.get("/new-table/paste")
def paste() -> str:
    return render_template(
        "new-table.html",
        method="paste",
        ordered_licences=ORDERED_LICENCES,
        action_url=url_for("create_table.new_table_form_submission"),
        page_title="Paste a new table",
    )


@bp.get("/new-table/upload-file")
def upload_file() -> str:
    return render_template(
        "new-table.html",
        method="upload-file",
        ordered_licences=ORDERED_LICENCES,
        Encoding=Encoding,
        action_url=url_for("create_table.new_table_form_submission"),
        page_title="Upload a new table",
    )


def canonicalise_git_url(input_url: str) -> str:
    """We allow various different forms for git urls to be passed by users.

    However we canonicalise to "smart HTTP" which is the protocol csvbase uses.

    """
    try:
        git_url = giturlparse.parse(input_url)
        if git_url.platform != "github":
            raise exc.InvalidRequest()

        parsed_url = urlparse(git_url.url2https)
        # this is necessary because giturlparse strips access tokens and we
        # need to put them back
        if hasattr(git_url, "access_token") and git_url.access_token != "":
            netloc_with_auth = (
                f"{git_url.username}:{git_url.access_token}@{parsed_url.netloc}"
            )
            parsed_url = ParseResult(
                scheme=parsed_url.scheme,
                netloc=netloc_with_auth,
                path=parsed_url.path,
                query=parsed_url.query,
                params=parsed_url.params,
                fragment=parsed_url.fragment,
            )
        return parsed_url.geturl()
    except AttributeError:
        logger.warning("unable to parse git url: '%s'", input_url)
        raise exc.InvalidRequest()


class CreateTableFromGit(MethodView):
    def get(self) -> str:
        return render_template(
            "create-table-git.html",
            method="git",
            action_url=url_for("create_table.from_git"),
            ordered_licences=ORDERED_LICENCES,
            branch="main",  # a nice default
        )

    def post(self) -> Response:
        source = GitSource()
        form = request.form
        repo = canonicalise_git_url(form["repo"])
        branch = form["branch"]
        path = form["path"]
        with source.retrieve(repo, branch, path) as git_file:
            str_buf = streams.byte_buf_to_str_buf(git_file.filelike)
            dialect, columns = streams.peek_csv(str_buf)

            with streams.rewind(git_file.filelike):
                file_id = temp.store_temp_file(git_file.filelike)

        licence = licence_form_field_to_licence(request.form.get("licence", None))
        github_source = GitUpstream(
            last_modified=git_file.version.last_changed,
            last_sha=bytes.fromhex(git_file.version.version_id),
            repo_url=repo,
            path=path,
            branch=branch,
        )
        private = "private" in request.form
        confirm_package = {
            "follow": github_source.to_json_dict(),
            "table_name": form["table-name"],
            "file_id": file_id,
            "is_public": not private,
            "licence": licence,
            "columns": [[c.name, c.type_.value] for c in columns],
        }
        token = secrets.token_urlsafe()
        response = redirect(url_for("create_table.confirm", token=token))
        response.set_cookie(
            f"confirm-token-{token}",
            dict_to_cookie(confirm_package),
            max_age=temp.DEFAULT_RETENTION,
            secure=True,
            httponly=True,
        )
        return response


def dict_to_cookie(d: Mapping) -> str:
    """Turn a json-able dict into a smallish cookie"""
    return b64encode(zlib.compress(json.dumps(d).encode("utf-8"))).decode("utf-8")


def cookie_to_dict(cookie: str) -> Dict:
    """Read cookie back into a dict"""
    # FIXME: catch errors, raise our of our exceptions
    return json.loads(zlib.decompress(b64decode(cookie)))


bp.add_url_rule(
    "/new-table/git", "from_git", view_func=CreateTableFromGit.as_view("from_git")
)


class CreateTableConfirm(MethodView):
    def get(self, token) -> str:
        # FIXME: what if the cookie is unset?
        confirm_package = cookie_to_dict(request.cookies[f"confirm-token-{token}"])
        columns = [Column(c[0], ColumnType(c[1])) for c in confirm_package["columns"]]
        return render_template(
            "create-table-confirm.html",
            page_title="Confirm table structure",
            columns=columns,
            ColumnType=ColumnType,
        )

    def post(self, token) -> Response:
        sesh = get_sesh()
        current_user = get_current_user_or_401()
        confirm_package = cookie_to_dict(request.cookies[f"confirm-token-{token}"])
        column_names = enumerate([c[0] for c in confirm_package["columns"]], start=1)
        columns = []
        for column_index, column_name in column_names:
            column_type = ColumnType[request.form[f"column-{column_index}-type"]]
            columns.append(Column(column_name, column_type))

        table_name = confirm_package["table_name"]
        licence = licence_form_field_to_licence(request.form.get("licence", None))
        table_uuid = svc.create_table_metadata(
            sesh,
            current_user.user_uuid,
            table_name,
            confirm_package["is_public"],
            "",
            licence,
            Backend.POSTGRES,
        )
        unique_columns = [
            c for c in columns if c.name in request.form.getlist("unique-columns")
        ]
        if len(unique_columns) > 0:
            svc.set_key(sesh, table_uuid, unique_columns)
        source = GitUpstream.from_json_dict(confirm_package["follow"])
        gh = GitSource()
        with gh.retrieve(source.repo_url, source.branch, source.path) as gh_f:

            # Correct these if they have changed since the upload was created
            source.last_sha = bytes.fromhex(gh_f.version.version_id)
            source.last_modified = gh_f.version.last_changed

            svc.create_git_upstream(sesh, table_uuid, source)

            backend = PGUserdataAdapter(sesh)
            backend.create_table(table_uuid, columns)
            str_buf = streams.byte_buf_to_str_buf(gh_f.filelike)
            dialect = streams.sniff_csv(str_buf)
            rows = table_io.csv_to_rows(str_buf, columns, dialect)
            table = svc.get_table(sesh, current_user.username, table_name)
            backend.insert_table_data(table, columns, rows)
        svc.mark_table_changed(sesh, table.table_uuid)
        sesh.commit()
        return redirect(
            url_for(
                "csvbase.table_view",
                username=current_user.username,
                table_name=table_name,
            )
        )


bp.add_url_rule(
    "/new-table/confirm/<token>",
    "confirm",
    view_func=CreateTableConfirm.as_view("confirm"),
)


@bp.post("/new-table")
def new_table_form_submission() -> Response:
    sesh = get_sesh()
    current_user = get_current_user_or_401()

    quota = billing_svc.get_quota(sesh, current_user.user_uuid)
    usage = svc.get_usage(sesh, current_user.user_uuid)
    private = "private" in request.form
    if private:
        usage.private_tables += 1
    else:
        usage.public_tables += 1
    if usage.exceeds_quota(quota):
        logger.warning("%s tried to exceed quota", current_user)
        raise exc.NotEnoughQuotaException()

    table_name = request.form["table-name"]
    csv_buf: UserSubmittedCSVData

    is_public = not private
    licence = licence_form_field_to_licence(request.form.get("licence"))
    table_uuid = svc.create_table_metadata(
        sesh,
        current_user.user_uuid,
        table_name,
        is_public,
        "",
        licence,
        Backend.POSTGRES,
    )

    backend = PGUserdataAdapter(sesh)

    textarea = request.form.get("csv-textarea")
    if textarea:
        csv_buf = io.StringIO(textarea)
    else:
        byte_buf = request.files["csv-file"]
        encoding = request.form.get("encoding", type=Encoding)
        csv_buf = streams.byte_buf_to_str_buf(byte_buf, encoding)

    try:
        dialect, columns = streams.peek_csv(csv_buf)
        backend.create_table(table_uuid, columns)
        rows = table_io.csv_to_rows(csv_buf, columns, dialect)
    except UnicodeDecodeError as e:
        raise exc.WrongEncodingException() from e

    table = svc.get_table(sesh, current_user.username, table_name)
    backend.insert_table_data(
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
        method="blank",
        action_url=url_for("create_table.blank_table_form_post"),
        ordered_licences=ORDERED_LICENCES,
        cols=cols,
        ColumnType=ColumnType,
        table_name=table_name,
    )


@bp.post("/new-table/blank")
def blank_table_form_post() -> Response:
    sesh = get_sesh()
    current_user = get_current_user_or_401()

    quota = billing_svc.get_quota(sesh, current_user.user_uuid)
    usage = svc.get_usage(sesh, current_user.user_uuid)
    if "private" in request.form:
        usage.private_tables += 1
    else:
        usage.public_tables += 1
    if usage.exceeds_quota(quota):
        logger.warning("%s tried to exceed quota", current_user)
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
    licence = licence_form_field_to_licence(request.form.get("licence", None))
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
        Backend.POSTGRES,
    )
    backend = PGUserdataAdapter(sesh)
    backend.create_table(table_uuid, cols)
    sesh.commit()
    return redirect(
        url_for(
            "csvbase.table_view",
            username=current_user.username,
            table_name=table_name,
        )
    )
