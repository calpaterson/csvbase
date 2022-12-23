import codecs
import io
import json
import secrets
import shutil
from datetime import date, timedelta
from logging import getLogger
from os import environ
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple, Union, cast
from urllib.parse import urlsplit, urlunsplit
from uuid import UUID

import werkzeug.http
from cchardet import UniversalDetector
from flask import (
    Blueprint,
    Flask,
    current_app,
    flash,
    g,
    jsonify,
    make_response,
    redirect,
    render_template,
    request,
)
from flask import session as flask_session
from flask import url_for
from flask.wrappers import Response as FlaskResponse
from flask_babel import Babel
from flask_cors import cross_origin
from flask_sqlalchemy_session import flask_scoped_session
from passlib.context import CryptContext
from sqlalchemy.orm import Session, sessionmaker
from typing_extensions import Literal
from werkzeug.routing import BaseConverter
from werkzeug.wrappers.response import Response

from . import blog, db, exc, svc
from .markdown import render_markdown
from .logging import configure_logging
from .sentry import configure_sentry
from .sesh import get_sesh
from .userdata import PGUserdataAdapter
from .value_objs import (
    ROW_ID_COLUMN,
    Column,
    ColumnType,
    ContentType,
    DataLicence,
    KeySet,
    Page,
    Row,
    Table,
    User,
    UserSubmittedBytes,
    UserSubmittedCSVData,
)
from .constants import COPY_BUFFER_SIZE

logger = getLogger(__name__)

bp = Blueprint("csvbase", __name__)

CORS_EXPIRY = timedelta(hours=8)

EXCEPTION_MESSAGE_CODE_MAP = {
    exc.UserDoesNotExistException: ("that user does not exist", 404),
    exc.RowDoesNotExistException: ("that row does not exist", 404),
    exc.TableDoesNotExistException: ("that table does not exist", 404),
    exc.NotAuthenticatedException: ("you need to sign in to do that", 401),
    exc.NotAllowedException: ("that's not allowed", 403),
    exc.WrongAuthException: ("that's the wrong password or api key", 400),
    exc.InvalidAPIKeyException: ("invalid api key", 400),
    exc.InvalidRequest: ("invalid request", 400),
    exc.CantNegotiateContentType: ("can't agree with you on a content type", 406),
    exc.WrongContentType: ("you sent the wrong content type", 400),
    exc.ProhibitedUsernameException: ("that username is not allowed", 400),
}


def init_app():
    configure_logging()
    configure_sentry()
    app = Flask(__name__)
    app.config["CRYPT_CONTEXT"] = CryptContext(["argon2"])
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_NAME"] = "csvbase_websesh"
    app.config["JSONIFY_PRETTYPRINT_REGULAR"] = True
    if "CSVBASE_SECRET_KEY" in environ:
        app.config["SECRET_KEY"] = environ["CSVBASE_SECRET_KEY"]
    else:
        app.logger.warning("CSVBASE_SECERT_KEY not set, using a random secret")
        app.config["SECRET_KEY"] = secrets.token_hex()

    Babel(app, default_locale="en_GB", default_timezone="Europe/London")

    class TableNameConverter(BaseConverter):
        regex = r"[A-z][-A-z0-9]+"

    app.url_map.converters["table_name"] = TableNameConverter

    app.register_blueprint(bp)

    if "CSVBASE_BLOG_REF" in environ:
        app.register_blueprint(blog.bp)

    @app.context_processor
    def inject_heroku():
        return dict(HEROKU="HEROKU" in environ)

    @app.context_processor
    def inject_blueprints():
        return dict(blueprints=app.blueprints.keys())

    app.jinja_env.filters["snake_case"] = snake_case
    app.jinja_env.filters["ppjson"] = ppjson

    sesh = flask_scoped_session(sessionmaker(bind=db.engine))
    sesh.init_app(app)

    # typing for errorhandler is apparently tricky...
    # https://github.com/pallets/flask/blob/bd56d19b167822a9a23e2e9e2a07ccccc36baa8d/src/flask/typing.py#L49
    @app.errorhandler(exc.CSVBaseException)  # type: ignore
    def handle_csvbase_exceptions(e: exc.CSVBaseException) -> Response:
        try:
            message, http_code = EXCEPTION_MESSAGE_CODE_MAP[e.__class__]
        except KeyError:
            # An exception we don't have a canned response for - reraise it
            raise e
        if is_browser():
            if http_code == 401:
                flash("You need to sign in to do that")
                return redirect(url_for("csvbase.register"))
            else:
                resp = make_response(f"http error code {http_code}: {message}")
                resp.status_code = http_code
                return resp
        else:
            resp = jsonify({"error": message})
            resp.status_code = http_code
            return resp

    @app.before_request
    def put_user_in_g() -> None:
        app_logger = current_app.logger
        user_uuid: Optional[Any] = flask_session.get("user_uuid")
        auth = request.authorization
        if user_uuid is not None:
            if not isinstance(user_uuid, UUID):
                del flask_session["user_uuid"]
                app_logger.warning("cleared a corrupt user_uuid cookie: %s", user_uuid)
            else:
                sesh = get_sesh()
                try:
                    user = svc.user_by_user_uuid(sesh, user_uuid)
                except exc.UserDoesNotExistException:
                    del flask_session["user_uuid"]
                    app_logger.warning(
                        "cleared a corrupt user_uuid cookie: %s", user_uuid
                    )
                else:
                    set_current_user(user)
                    app_logger.debug(
                        "currently signed in as: %s", g.current_user.username
                    )

        elif auth is not None:
            sesh = get_sesh()
            username = auth.username or ""
            if svc.is_valid_api_key(sesh, username, auth.password or ""):
                user = svc.user_by_name(sesh, username)
                set_current_user(user)
            else:
                raise exc.WrongAuthException()
        else:
            app_logger.debug("not signed in")

    @app.after_request
    def set_default_cache_control(response: FlaskResponse) -> FlaskResponse:
        cc = response.cache_control
        if len(cc.values()) == 0:
            # nothing specific has been set, so set the default
            cc.no_store = True
        return response

    return app


@bp.route("/")
def index() -> str:
    sesh = get_sesh()
    return render_template("index.html", top_ten=svc.get_top_n(sesh))


@bp.route("/about")
def about() -> str:
    return render_template("about.html")


@bp.route("/new-table/paste")
def paste() -> str:
    return render_template(
        "new-table.html",
        method="paste",
        DataLicence=DataLicence,
        action_url=url_for("csvbase.new_table_form_submission"),
    )


@bp.route("/new-table/upload-file", methods=["GET"])
def upload_file() -> str:
    return render_template(
        "new-table.html",
        method="upload-file",
        DataLicence=DataLicence,
        action_url=url_for("csvbase.new_table_form_submission"),
    )


@bp.route("/new-table", methods=["POST"])
def new_table_form_submission() -> Response:
    sesh = get_sesh()
    if "username" in request.form:
        user = svc.create_user(
            sesh,
            current_app.config["CRYPT_CONTEXT"],
            request.form["username"],
            request.form["password"],
            request.form.get("email"),
        )
        set_current_user_for_session(user)
        flash("Account registered")
    else:
        am_a_user_or_400()
        user = g.current_user

    table_name = request.form["table-name"]
    textarea = request.form.get("csv-textarea")
    csv_buf: UserSubmittedCSVData
    if textarea:
        csv_buf = io.StringIO(textarea)
    else:
        csv_buf = byte_buf_to_str_buf(request.files["csv-file"])
    if "private" in request.form:
        is_public = False
    else:
        is_public = True
    data_licence = DataLicence(request.form.get("data-licence", type=int))
    dialect, columns = svc.peek_csv(csv_buf)
    csv_buf.seek(0)
    table_uuid = PGUserdataAdapter.create_table(
        sesh, g.current_user.username, table_name, columns
    )
    svc.create_table_metadata(
        sesh,
        table_uuid,
        g.current_user.user_uuid,
        table_name,
        is_public,
        "",
        data_licence,
    )
    table = svc.get_table(sesh, user.username, table_name)
    # FIXME: what happens if this fails?
    PGUserdataAdapter.insert_table_data(
        sesh,
        g.current_user.user_uuid,
        g.current_user.username,
        table,
        csv_buf,
        dialect,
        columns,
    )
    sesh.commit()
    return redirect(
        url_for(
            "csvbase.table_view",
            username=g.current_user.username,
            table_name=table_name,
        )
    )


@bp.route("/new-table/blank", methods=["GET"])
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

    return render_template(
        "new-blank-table.html",
        action_url=url_for("csvbase.blank_table_form_post"),
        DataLicence=DataLicence,
        cols=cols,
        ColumnType=ColumnType,
    )


@bp.route("/new-table/blank", methods=["POST"])
def blank_table_form_post() -> Response:
    am_a_user_or_400()
    sesh = get_sesh()
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
    licence = DataLicence(request.form.get("licence", type=int))
    if "private" in request.form:
        is_public = False
    else:
        is_public = True

    table_uuid = PGUserdataAdapter.create_table(
        sesh, g.current_user.username, table_name, cols
    )
    svc.create_table_metadata(
        sesh,
        table_uuid,
        g.current_user.user_uuid,
        table_name,
        is_public,
        "",
        licence,
    )
    sesh.commit()
    return redirect(
        url_for(
            "csvbase.table_view",
            username=g.current_user.username,
            table_name=table_name,
        )
    )


@bp.route("/<username>/<table_name:table_name>", methods=["GET"])
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT"])
def table_view(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(user.username):
        raise exc.TableDoesNotExistException(username, table_name)

    content_type = negotiate_content_type(
        [ContentType.HTML, ContentType.JSON], default=ContentType.CSV
    )

    table = svc.get_table(sesh, username, table_name)

    return make_table_view_response(sesh, user, content_type, table)


@bp.route("/<username>/<table_name:table_name>.<extension>", methods=["GET"])
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

    return make_table_view_response(sesh, user, content_type, table)


def make_table_view_response(sesh, user, content_type, table):
    if content_type is ContentType.HTML:
        keyset = keyset_from_request_args()
        page = PGUserdataAdapter.table_page(sesh, user.username, table, keyset)
        return make_response(
            render_template(
                "table_view.html",
                page_title=f"{user.username}/{table.table_name}",
                table=table,
                page=page,
                keyset=keyset,
                ROW_ID_COLUMN=ROW_ID_COLUMN,
                praise_id=get_praise_id_if_exists(table),
            )
        )
    elif content_type is ContentType.JSON:
        keyset = keyset_from_request_args()
        page = PGUserdataAdapter.table_page(sesh, user.username, table, keyset)
        return jsonify(table_to_json_dict(table, page))
    elif content_type is ContentType.PARQUET:
        return make_streaming_response(
            svc.table_as_parquet(sesh, table.table_uuid), "application/octet-stream"
        )
    elif content_type is ContentType.JSON_LINES:
        return make_streaming_response(
            svc.table_as_jsonlines(sesh, table.table_uuid), "text/plain"
        )
    else:
        return make_streaming_response(
            svc.table_as_csv(sesh, table.table_uuid), ContentType.CSV.value
        )


@bp.route("/<username>/<table_name:table_name>/readme", methods=["GET"])
def table_readme(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(username):
        raise exc.TableDoesNotExistException(username, table_name)

    readme_markdown = svc.get_readme_markdown(sesh, user.user_uuid, table_name)
    if readme_markdown is not None:
        readme_html = render_markdown(readme_markdown)
    else:
        readme_html = "(no readme set)"

    table = svc.get_table(sesh, username, table_name)
    return make_response(
        render_template(
            "table_readme.html",
            page_title=f"Readme for {username}/{table_name}",
            table=table,
            table_readme=readme_html,
            praise_id=get_praise_id_if_exists(table),
        )
    )


@bp.route("/<username>/<table_name:table_name>/docs", methods=["GET"])
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


@bp.route("/<username>/<table_name:table_name>/export", methods=["GET"])
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


@bp.route("/<username>/<table_name:table_name>/details", methods=["GET"])
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


@bp.route("/<username>/<table_name:table_name>/settings", methods=["GET"])
def table_settings(username: str, table_name: str) -> str:
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    table = svc.get_table(sesh, username, table_name)
    am_user_or_400(username)

    table_readme_markdown = svc.get_readme_markdown(sesh, user.user_uuid, table_name)

    return render_template(
        "table_settings.html",
        username=username,
        page_title=f"Settings: {username}/{table_name}",
        table_readme=table_readme_markdown or "",
        DataLicence=DataLicence,
        table=table,
        praise_id=get_praise_id_if_exists(table),
    )


@bp.route(
    "/<username>/<table_name:table_name>/delete-table-form-post", methods=["POST"]
)
def delete_table_form_post(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    user = svc.user_by_name(sesh, username)
    table = svc.get_table(sesh, username, table_name)
    am_user_or_400(username)
    svc.delete_table_and_metadata(sesh, username, table_name)
    sesh.commit()
    flash(f"Deleted {username}/{table_name}")
    return redirect(url_for("csvbase.user", username=username))


@bp.route("/<username>/<table_name:table_name>/settings", methods=["POST"])
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
    sesh.commit()

    flash(f"Saved settings for {username}/{table_name}")

    return redirect(
        url_for(
            "csvbase.table_settings",
            username=username,
            table_name=table_name,
        )
    )


@bp.route("/<username>/<table_name:table_name>/praise", methods=["POST"])
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


@bp.route("/<username>/<table_name:table_name>.csv", methods=["GET"])
def get_table_csv(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    return make_streaming_response(
        svc.table_as_csv(sesh, table.table_uuid), ContentType.CSV.value
    )


@bp.route("/<username>/<table_name:table_name>.json", methods=["GET"])
def table_view_json(username: str, table_name: str) -> Tuple[str, int]:
    return "not implemented", 501


@bp.route("/<username>/<table_name:table_name>.xlsx", methods=["GET"])
def get_table_xlsx(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    return make_streaming_response(
        svc.table_as_xlsx(sesh, table.table_uuid), ContentType.CSV.value
    )


@bp.route("/<username>/<table_name:table_name>/export/xlsx", methods=["GET"])
def export_table_xlsx(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    excel_table = "excel-table" in request.args

    xlsx_buf = svc.table_as_xlsx(sesh, table.table_uuid, excel_table=excel_table)

    return make_streaming_response(
        xlsx_buf,
        make_download_filename(username, table_name, "xlsx"),
        ContentType.XLSX.value,
    )


CSV_SEPARATOR_MAP: Dict[str, str] = {"comma": ",", "tab": "\t", "vertical-bar": "|"}


@bp.route("/<username>/<table_name:table_name>/export/csv", methods=["GET"])
def export_table_csv(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    separator = request.args.get("separator", "comma")
    try:
        delimiter = CSV_SEPARATOR_MAP[separator]
    except KeyError:
        raise exc.InvalidRequest(f"invalid separator: {separator}")

    csv_buf = svc.table_as_csv(
        sesh,
        table.table_uuid,
        delimiter=delimiter,
    )

    extension = "tsv" if separator == "tab" else "csv"
    return make_streaming_response(
        csv_buf,
        make_download_filename(username, table_name, extension),
        ContentType.CSV.value,
    )


@bp.route("/<username>/<table_name:table_name>/rows/", methods=["POST"])
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
            c: c.type_.from_html_form_to_python(request.form.get(c.name))
            for c in table.user_columns()
        }
    else:
        raise exc.WrongContentType(
            [ContentType.JSON, ContentType.HTML_FORM], request.mimetype
        )

    row_id = PGUserdataAdapter.insert_row(sesh, table.table_uuid, row)
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
                n=row_id - 1,
            )
        )


@bp.route("/<username>/<table_name:table_name>/rows/<int:row_id>", methods=["GET"])
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
                "row_view_or_edit.html",
                page_title=f"{username}/{table_name}/rows/{row_id}",
                row=row,
                row_id=row_id,
                table=table,
            )
        )
    else:
        table = svc.get_table(sesh, username, table_name)
        return jsonify(row_to_json_dict(table, row))


@bp.route(
    "/<username>/<table_name:table_name>/add-row-form",
    methods=["GET"],
)
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


@bp.route(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/delete-check",
    methods=["GET"],
)
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


@bp.route("/<username>/<table_name:table_name>/rows/<int:row_id>", methods=["PUT"])
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT", "DELETE"])
def update_row(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)

    body = json_or_400()
    if body["row_id"] != row_id:
        raise exc.InvalidRequest("can't change row ids via an update")
    row = {
        c: c.type_.from_json_to_python(body["row"][c.name])
        for c in table.user_columns()
    }
    row[table.row_id_column()] = row_id

    if not PGUserdataAdapter.update_row(sesh, table.table_uuid, row_id, row):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    sesh.commit()
    return jsonify(body)


@bp.route("/<username>/<table_name:table_name>/rows/<int:row_id>", methods=["DELETE"])
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT", "DELETE"])
def delete_row(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)
    if not PGUserdataAdapter.delete_row(sesh, table.table_uuid, row_id):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    sesh.commit()
    response = make_response()
    response.status_code = 204
    return response


@bp.route(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/delete-row-for-browsers",
    methods=["POST"],
)
def delete_row_for_browsers(username: str, table_name: str, row_id: int) -> Response:
    # extremely annoying to need a special endpoint for this but browser forms
    # don't support the DELETE verb
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    table = svc.get_table(sesh, username, table_name)
    if not PGUserdataAdapter.delete_row(sesh, table.table_uuid, row_id):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    sesh.commit()
    flash(f"Deleted row {row_id}")
    return redirect(
        url_for("csvbase.table_view", username=username, table_name=table_name)
    )


@bp.route(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/edit", methods=["POST"]
)
def update_row_by_form_post(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    table = svc.get_table(sesh, username, table_name)
    row: Row = {
        c: c.type_.from_html_form_to_python(request.form.get(c.name))
        for c in table.columns
    }
    PGUserdataAdapter.update_row(sesh, table.table_uuid, row_id, row)
    sesh.commit()
    flash(f"Updated row {row_id}")
    return redirect(
        url_for(
            "csvbase.get_row", username=username, table_name=table_name, row_id=row_id
        )
    )


# FIXME: assert table name and user name match regex
@bp.route("/<username>/<table_name:table_name>", methods=["PUT"])
@cross_origin(max_age=CORS_EXPIRY, methods=["GET", "PUT"])
def upsert_table(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    am_user_or_400(username)
    # FIXME: add checking for forms here
    byte_buf = io.BytesIO()
    shutil.copyfileobj(request.stream, byte_buf)
    str_buf = byte_buf_to_str_buf(byte_buf)
    dialect, _ = svc.sniff_csv(str_buf)
    table = svc.get_table(sesh, username, table_name)

    str_buf.seek(0)
    PGUserdataAdapter.upsert_table_data(
        sesh,
        table,
        str_buf,
        dialect,
    )
    sesh.commit()
    return make_text_response(f"upserted {username}/{table_name}")


@bp.route("/<username>", methods=["GET"])
def user(username: str) -> Response:
    sesh = get_sesh()
    include_private = False
    if "current_user" in g and g.current_user.username == username:
        include_private = True
    user = svc.user_by_name(sesh, username)
    tables = svc.tables_for_user(
        sesh,
        username,
        include_private=include_private,
    )
    return make_response(
        render_template(
            "user.html",
            user=user,
            page_title=f"{username}",
            tables=list(tables),
        )
    )


@bp.route("/robots.txt", methods=["GET"])
def robots() -> Response:
    sitemap_url = url_for("csvbase.sitemap", _external=True)
    robots_doc = f"Sitemap: {sitemap_url}"
    resp = make_response(robots_doc)
    resp.cache_control.public = True
    resp.cache_control.max_age = int(timedelta(days=1).total_seconds())
    return resp


@bp.route("/sitemap.xml", methods=["GET"])
def sitemap() -> Response:
    sesh = get_sesh()
    table_names = svc.get_public_table_names(sesh)
    # FIXME: include blog urls
    table_urls = (
        url_for(
            "csvbase.table_view",
            username=username,
            table_name=table_name,
            _external=True,
        )
        for username, table_name in table_names
    )
    resp = make_response(render_template("sitemap.xml", urls=table_urls))
    resp.mimetype = "application/xml"
    resp.cache_control.public = True
    resp.cache_control.max_age = int(timedelta(days=1).total_seconds())
    return resp


@bp.route("/register", methods=["GET", "POST"])
def register() -> Response:
    if request.method == "GET":
        return make_response(render_template("register.html", whence=request.referrer))
    else:
        sesh = get_sesh()
        user = svc.create_user(
            sesh,
            current_app.config["CRYPT_CONTEXT"],
            request.form["username"],
            request.form["password"],
            request.form.get("email"),
        )
        sesh.commit()
        set_current_user_for_session(user)
        flash("Account registered")
        whence = request.form.get(
            "whence", url_for("csvbase.user", username=user.username)
        )
        return redirect(whence)


@bp.route("/sign-in", methods=["GET", "POST"])
def sign_in() -> Response:
    if request.method == "GET":
        return make_response(render_template("sign_in.html", whence=request.referrer))
    else:
        sesh = get_sesh()
        username = request.form["username"]
        if svc.is_correct_password(
            sesh,
            current_app.config["CRYPT_CONTEXT"],
            username,
            request.form["password"],
        ):
            set_current_user_for_session(
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


@bp.route("/sign-out", methods=["GET"])
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


def make_text_response(text: str, status=200):
    # need a trailing newline else shells get confused
    resp = make_response("200 OK: " + text + "\n")
    resp.headers["Content-Type"] = "text/plain"
    return resp


def make_download_filename(username: str, table_name: str, extension: str) -> str:
    timestamp = date.today().isoformat()
    return f"{table_name}-{timestamp}.{extension}"


def make_streaming_response(
    parquet_buf: Union[io.BytesIO, io.StringIO],
    mimetype: str,
    download_filename: Optional[str] = None,
) -> Response:
    def generate():
        minibuf = parquet_buf.read(COPY_BUFFER_SIZE)
        while minibuf:
            yield minibuf
            minibuf = parquet_buf.read(COPY_BUFFER_SIZE)

    response = current_app.response_class(generate(), mimetype=mimetype)
    if download_filename is not None:
        response.headers[
            "Content-Disposition"
        ] = f'attachment; filename="{download_filename}"'
    return response


def is_browser() -> bool:
    # bit of content negotiation magic
    accepts = werkzeug.http.parse_accept_header(request.headers.get("Accept"))
    best = accepts.best_match(["text/html", "text/csv"], default="text/csv")
    return best == "text/html"


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


def byte_buf_to_str_buf(byte_buf: UserSubmittedBytes) -> codecs.StreamReader:
    """Convert a readable byte buffer into a readable str buffer.

    Tries to detect the character set along the way, falling back to utf-8."""
    detector = UniversalDetector()
    for line in byte_buf.readlines():
        detector.feed(line)
        if detector.done:
            break
        if byte_buf.tell() > 1_000_000:
            logger.warning("unable to detect after 1mb, giving up")
            break
    logger.info("detected: %s after %d bytes", detector.result, byte_buf.tell())
    byte_buf.seek(0)
    if detector.result["encoding"] is not None:
        encoding = detector.result["encoding"]
    else:
        logger.warning("unable to detect charset, assuming utf-8")
        encoding = "utf-8"
    Reader = codecs.getreader(encoding)
    return Reader(byte_buf)


def set_current_user_for_session(user: User, session: Optional[Any] = None) -> None:
    """Sets the current user and creates a web session."""
    set_current_user(user)

    if session is None:
        session = flask_session
    session["user_uuid"] = user.user_uuid
    # Make it last for 31 days
    session.permanent = True


def set_current_user(user: User):
    g.current_user = user

    # This is duplication but very convenient for jinja templates
    g.current_username = user.username


def json_or_400() -> Dict[str, Any]:
    if request.json is None:
        raise exc.InvalidRequest()
    else:
        return request.json


def snake_case(inp: str) -> str:
    # FIXME: this ignores capitalisations...
    return inp.replace("-", "_")


def ppjson(inp: Union[Mapping, Sequence]) -> str:
    return json.dumps(inp, indent=4)


def keyset_from_request_args() -> KeySet:
    n: int = request.args.get("n", default=0, type=int)
    op: Literal["greater_than", "less_than"] = (
        "greater_than" if request.args.get("op", default="gt") == "gt" else "less_than"
    )
    keyset = KeySet(n=n, op=op)
    return keyset


def row_to_json_dict(table: Table, row: Row, omit_row_id=False) -> Dict[str, Any]:
    # FIXME: rename "omit_row_id" to "as_though_unsubmitted" or something
    row_without_row_id = (c for c in row.items() if c[0].name != "csvbase_row_id")
    json_dict: Dict = {
        "row": {
            column.name: column.type_.value_to_json(value)
            for column, value in row_without_row_id
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
        "columns": [
            {"name": column.name, "type": column.type_.pretty_type()}
            for column in table.columns
        ],
        "page": page_to_json_dict(table, page),
    }
    return rv


def url_for_with_auth(endpoint: str, **values) -> str:
    """Build a url, but add basic auth (if the users is logged in"""
    flask_url = url_for(endpoint, **values)
    if "current_user" in g:
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
