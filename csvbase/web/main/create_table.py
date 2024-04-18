"""Machinery for the creation of new tables."""

import io
from logging import getLogger
from typing import (
    List,
    Tuple,
)
from urllib.parse import urlparse

from flask.views import MethodView
from flask import Blueprint, redirect, render_template, request, url_for, request
from werkzeug.wrappers.response import Response

from ..func import get_current_user_or_401, register_and_sign_in_new_user
from ... import exc, svc, streams, table_io
from ...sesh import get_sesh
from ...userdata import PGUserdataAdapter
from ...userdata.gituserdata import GithubUserdataAdapter
from ...value_objs import (
    Column,
    ColumnType,
    Encoding,
    DataLicence,
    Backend,
)
from ...streams import UserSubmittedCSVData
from ..billing import svc as billing_svc

bp = Blueprint("create_table", __name__)

logger = getLogger(__name__)


@bp.route("/new-table/paste")
def paste() -> str:
    return render_template(
        "new-table.html",
        method="paste",
        DataLicence=DataLicence,
        action_url=url_for("create_table.new_table_form_submission"),
        page_title="Paste a new table",
    )


@bp.get("/new-table/upload-file")
def upload_file() -> str:
    return render_template(
        "new-table.html",
        method="upload-file",
        DataLicence=DataLicence,
        Encoding=Encoding,
        action_url=url_for("create_table.new_table_form_submission"),
        page_title="Upload a new table",
    )


def parse_github_url(repo: str) -> Tuple[str, str]:
    """Parse the org and repo out of a github url.

    This is designed to be forgiving, so supports a few different formats.

    """
    parsed = urlparse(repo)
    path = parsed.path.split("/")
    if len(path) < 3:
        raise exc.InvalidRequest("couldn't parse repo url")
    return path[1], path[2]


class CreateTableFromGit(MethodView):
    def get(self) -> str:
        return render_template(
            "create-table-git.html",
            method="from-git",
            action_url=url_for("create_table.from_git"),
            DataLicence=DataLicence,
            branch="main",  # a nice default
        )

    def post(self) -> Response:
        backend = GithubUserdataAdapter(None)
        form = request.form
        org, repo = parse_github_url(form["repo"])
        branch = form["branch"]
        path = form["path"]
        if True:
            github_file = backend.retrieve(org, repo, branch, path)
            str_buf = streams.byte_buf_to_str_buf(github_file.body)
            dialect, columns = streams.peek_csv(str_buf)
            return render_template(
                "create-table-git.html",
                method="from-git",
                action_url=url_for("create_table.from_git"),
                DataLicence=DataLicence,
                repo=form["repo"],
                branch=branch,
                path=path,
                dialect=dialect,
                columns=columns,
            )


bp.add_url_rule(
    "/new-table/git", "from_git", view_func=CreateTableFromGit.as_view("from_git")
)


@bp.post("/new-table")
def new_table_form_submission() -> Response:
    sesh = get_sesh()
    if "username" in request.form:
        current_user = register_and_sign_in_new_user(sesh)
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
        logger.warning("%s tried to exceed quota", current_user)
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
        table = svc.get_table(sesh, current_user.username, table_name)
        rows = table_io.csv_to_rows(csv_buf, columns, dialect)
    except UnicodeDecodeError as e:
        raise exc.WrongEncodingException() from e

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
        action_url=url_for("create_table.blank_table_form_post"),
        DataLicence=DataLicence,
        cols=cols,
        ColumnType=ColumnType,
        table_name=table_name,
    )


@bp.post("/new-table/blank")
def blank_table_form_post() -> Response:
    sesh = get_sesh()
    if "username" in request.form:
        current_user = register_and_sign_in_new_user(sesh)
    else:
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
