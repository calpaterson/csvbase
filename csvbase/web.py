from uuid import UUID
from datetime import date
import io
import shutil
import codecs
from logging import basicConfig, INFO, getLogger
from typing import Optional, Any, Dict, List, Tuple
from os import environ
from urllib.parse import urlsplit

from typing_extensions import Literal
from cchardet import UniversalDetector
from werkzeug.wrappers.response import Response
from werkzeug.routing import BaseConverter
from flask import (
    g,
    session as flask_session,
    Flask,
    request,
    abort,
    make_response,
    render_template,
    redirect,
    url_for,
    Blueprint,
    current_app,
    flash,
    jsonify,
)
from passlib.context import CryptContext
from sqlalchemy.orm import sessionmaker, Session
from flask_sqlalchemy_session import flask_scoped_session
import werkzeug.http

from .value_objs import KeySet, ColumnType, PythonType, Column, User
from . import svc
from . import db
from . import exc


logger = getLogger(__name__)

bp = Blueprint("csvbase", __name__)


EXCEPTION_MESSAGE_CODE_MAP = {
    exc.UserDoesNotExistException: ("user does not exist", 404),
    exc.RowDoesNotExistException: ("row does not exist", 404),
    exc.TableDoesNotExistException: ("table does not exist", 404),
    exc.NotAuthenticatedException: ("not authenticated", 401),
    exc.NotAllowedException: ("not allowed", 403),
    exc.WrongAuthException: ("wrong auth", 400),
    exc.InvalidAPIKeyException: ("invalid api key", 400),
    exc.InvalidRequest: ("invalid request", 400),
}


def init_app():
    basicConfig(level=INFO)
    app = Flask(__name__)
    app.config["CRYPT_CONTEXT"] = CryptContext(["argon2"])
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_NAME"] = "csvbase_websesh"
    app.config["SECRET_KEY"] = "no peeking"

    app.url_map.converters["table_name"] = TableNameConverter

    app.register_blueprint(bp)

    db.make_tables()

    @app.context_processor
    def inject_heroku():
        return dict(HEROKU="HEROKU" in environ)

    app.jinja_env.filters["snake_case"] = snake_case

    sesh = flask_scoped_session(sessionmaker(bind=db.engine))
    sesh.init_app(app)

    return app


class TableNameConverter(BaseConverter):
    regex = r"[A-z][-A-z0-9]+"


@bp.errorhandler(exc.CSVBaseException)
def handle_csvbase_exceptions(e):
    message, http_code = EXCEPTION_MESSAGE_CODE_MAP[e.__class__]
    if is_browser():
        # web browsers handle 401 specially, use 400
        if http_code == 401:
            http_code = 400
        return f"{http_code}: {message}", http_code
    else:
        return jsonify({"error": message}), http_code


@bp.before_request
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
                app_logger.warning("cleared a corrupt user_uuid cookie: %s", user_uuid)
            else:
                set_current_user(user)
                app_logger.debug("currently signed in as: %s", g.username)

    elif auth is not None:
        sesh = get_sesh()
        if svc.is_valid_api_key(sesh, auth.username or "", auth.password or ""):
            user = svc.user_by_name(sesh, auth.username)
            set_current_user(user)
        else:
            raise exc.WrongAuthException()
    else:
        app_logger.debug("not signed in")


@bp.route("/")
def landing() -> str:
    return render_template("landing.html")


@bp.route("/new-table/paste")
def paste() -> str:
    return render_template(
        "new-table.html",
        method="paste",
        action_url=url_for("csvbase.new_table_form_submission"),
    )


@bp.route("/new-table/upload-file", methods=["GET"])
def upload_file() -> str:
    return render_template(
        "new-table.html",
        method="upload-file",
        action_url=url_for("csvbase.new_table_form_submission"),
    )


@bp.route("/new-table", methods=["POST"])
def new_table_form_submission():
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
        flash("Account created")
    else:
        am_a_user_or_400()

    table_name = request.form["table-name"]
    textarea = request.form.get("csv-textarea")
    if textarea:
        csv_buf = io.StringIO(textarea)
    else:
        csv_buf = byte_buf_to_str_buf(request.files["csv-file"])
    if "private" in request.form:
        public = False
    else:
        public = True
    svc.upsert_table(sesh, g.user_uuid, g.username, table_name, csv_buf, public=public)
    sesh.commit()
    return redirect(
        url_for("csvbase.get_table", username=g.username, table_name=table_name)
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
    svc.create_table(sesh, g.username, table_name, cols)
    svc.upsert_table_metadata(
        sesh,
        g.user_uuid,
        table_name,
        request.form.get("private", default=False, type=bool),
    )
    sesh.commit()
    return redirect(
        url_for("csvbase.get_table", username=g.username, table_name=table_name)
    )


@bp.route("/<username>/<table_name:table_name>", methods=["GET"])
def get_table(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    if is_browser():
        # passing a default and type here means the default is used if what they
        # provide can't be parsed
        n: int = request.args.get("n", default=0, type=int)
        op: Literal["greater_than", "less_than"] = (
            "greater_than"
            if request.args.get("op", default="gt") == "gt"
            else "less_than"
        )
        keyset = KeySet(n=n, op=op)

        cols = svc.get_columns(sesh, username, table_name, include_row_id=True)
        page = svc.table_page(sesh, user.user_uuid, username, table_name, keyset)
        return make_response(
            render_template(
                "table_view.html",
                cols=cols,
                page=page,
                keyset=keyset,
                username=username,
                table_name=table_name,
            )
        )
    else:
        return make_csv_response(
            svc.table_as_csv(sesh, user.user_uuid, username, table_name)
        )


@bp.route("/<username>/<table_name:table_name>/docs", methods=["GET"])
def get_table_apidocs(username: str, table_name: str) -> str:
    sesh = get_sesh()
    is_public = svc.is_public(sesh, username, table_name)
    is_public or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    table_url = url_for(
        "csvbase.get_table", username=username, table_name=table_name, _external=True
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
    if not is_public:
        table_url = private_table_url

    return render_template(
        "table_api.html",
        username=username,
        table_name=table_name,
        table_url=table_url,
        private_table_url=private_table_url,
        is_public=is_public,
    )


@bp.route("/<username>/<table_name:table_name>/export", methods=["GET"])
def table_export(username: str, table_name: str) -> str:
    sesh = get_sesh()
    is_public = svc.is_public(sesh, username, table_name)
    is_public or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    table_url = url_for(
        "csvbase.get_table", username=username, table_name=table_name, _external=True
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
    if not is_public:
        table_url = private_table_url

    return render_template(
        "table_export.html",
        username=username,
        table_name=table_name,
        table_url=table_url,
        private_table_url=private_table_url,
        is_public=is_public,
    )


@bp.route("/<username>/<table_name:table_name>.csv", methods=["GET"])
def get_table_csv(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    return make_csv_response(
        svc.table_as_csv(sesh, user.user_uuid, username, table_name)
    )


@bp.route("/<username>/<table_name:table_name>.xlsx", methods=["GET"])
def get_table_xlsx(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    return make_xlsx_response(
        svc.table_as_xlsx(sesh, user.user_uuid, username, table_name)
    )


@bp.route("/<username>/<table_name:table_name>/export/xlsx", methods=["GET"])
def export_table_xlsx(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    excel_table = "excel-table" in request.args

    xlsx_buf = svc.table_as_xlsx(
        sesh, user.user_uuid, username, table_name, excel_table=excel_table
    )

    return make_xlsx_response(
        xlsx_buf,
        make_download_filename(username, table_name, "xlsx"),
    )


CSV_SEPARATOR_MAP: Dict[str, str] = {"comma": ",", "tab": "\t", "vertical-bar": "|"}


@bp.route("/<username>/<table_name:table_name>/export/csv", methods=["GET"])
def export_table_csv(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user = svc.user_by_name(sesh, username)

    separator = request.args.get("separator", "comma")
    try:
        delimiter = CSV_SEPARATOR_MAP[separator]
    except KeyError:
        raise exc.InvalidRequest(f"invalid separator: {separator}")

    csv_buf = svc.table_as_csv(
        sesh,
        user.user_uuid,
        username,
        table_name,
        delimiter=delimiter,
    )

    extension = "tsv" if separator == "tab" else "csv"
    return make_csv_response(
        csv_buf,
        make_download_filename(username, table_name, extension),
    )


@bp.route("/<username>/<table_name:table_name>/rows/", methods=["POST"])
def create_row(username: str, table_name: str) -> Tuple[Response, int]:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    if not svc.is_public(sesh, username, table_name):
        raise exc.TableDoesNotExistException(username, table_name)
    if not am_user(username):
        if am_a_user():
            raise exc.NotAllowedException()
        else:
            raise exc.NotAuthenticatedException()
    body = json_or_400()
    assert "row_id" not in body
    row_id = svc.insert_row(sesh, username, table_name, body["row"])
    sesh.commit()
    body["row_id"] = row_id
    return jsonify(body), 201


@bp.route("/<username>/<table_name:table_name>/rows/<int:row_id>", methods=["GET"])
def get_row(username: str, table_name: str, row_id: int) -> Tuple[Response, int]:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(username):
        raise exc.TableDoesNotExistException(username, table_name)
    row = svc.get_row(sesh, username, table_name, row_id)
    if is_browser():
        return (
            make_response(
                render_template(
                    "row.html",
                    row=row,
                    row_id=row_id,
                    username=username,
                    table_name=table_name,
                )
            ),
            200,
        )
    else:
        return (
            jsonify(
                {
                    "row_id": row_id,
                    "row": {
                        column.name: column.type_.value_to_json(value)
                        for column, value in row.items()
                    },
                }
            ),
            200,
        )


@bp.route("/<username>/<table_name:table_name>/rows/<int:row_id>", methods=["PUT"])
def update_row(username: str, table_name: str, row_id: int) -> Tuple[str, int]:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    body = json_or_400()
    assert body["row_id"] == row_id, "row ids cannot be changed"
    if not svc.update_row(sesh, username, table_name, row_id, body["row"]):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    sesh.commit()
    return "", 204


@bp.route("/<username>/<table_name:table_name>/rows/<int:row_id>", methods=["DELETE"])
def delete_row(username: str, table_name: str, row_id: int) -> Tuple[str, int]:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    if not svc.delete_row(sesh, username, table_name, row_id):
        raise exc.RowDoesNotExistException(username, table_name, row_id)
    sesh.commit()
    return "", 204


@bp.route(
    "/<username>/<table_name:table_name>/rows/<int:row_id>/edit", methods=["POST"]
)
def update_row_by_form_post(username, table_name, row_id):
    sesh = get_sesh()
    columns = svc.get_columns(sesh, username, table_name)
    values = {
        c.name: c.type_.from_string_to_python(request.form[c.name]) for c in columns
    }
    svc.update_row(sesh, username, table_name, row_id, values)
    sesh.commit()
    flash(f"Updated row {row_id}")
    return redirect(
        url_for(
            "csvbase.get_row", username=username, table_name=table_name, row_id=row_id
        )
    )


# FIXME: assert table name and user name match regex
@bp.route("/<username>/<table_name:table_name>", methods=["PUT"])
def upsert_table(username, table_name):
    sesh = get_sesh()
    am_user_or_400(username)
    # FIXME: add checking for forms here
    byte_buf = io.BytesIO()
    shutil.copyfileobj(request.stream, byte_buf)
    str_buf = byte_buf_to_str_buf(byte_buf)
    svc.upsert_table(
        sesh, svc.user_by_name(sesh, username).user_uuid, username, table_name, str_buf
    )
    sesh.commit()
    return make_text_response(f"upserted {username}/{table_name}")


@bp.route("/<username>", methods=["GET"])
def user(username):
    sesh = get_sesh()
    tables = svc.tables_for_user(
        sesh,
        svc.user_by_name(sesh, username).user_uuid,
        include_private=g.get("username") == username,
    )
    return make_response(
        render_template(
            "user.html",
            username=username,
            table_names=tables,
        )
    )


@bp.route("/sign-in", methods=["GET", "POST"])
def sign_in() -> Response:
    sesh = get_sesh()
    if request.method == "GET":
        return make_response(render_template("sign_in.html", whence=request.referrer))
    else:
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
    return g.get("username", None) == username


def am_a_user() -> bool:
    return "username" in g


def am_user_or_400(username: str) -> bool:
    if not am_user(username):
        abort(400)
    return True


def am_a_user_or_400():
    if not am_a_user():
        abort(400)


def make_text_response(text: str, status=200):
    # need a trailing newline else shells get confused
    resp = make_response("200 OK: " + text + "\n")
    resp.headers["Content-Type"] = "text/plain"
    return resp


def make_download_filename(username: str, table_name: str, extension: str) -> str:
    timestamp = date.today().isoformat()
    return f"{table_name}-{timestamp}.{extension}"


def make_csv_response(
    csv_buf: io.StringIO, download_filename: Optional[str] = None
) -> Response:
    def generate():
        minibuf = csv_buf.read(4096)
        while minibuf:
            yield minibuf
            minibuf = csv_buf.read(4096)

    response = current_app.response_class(generate(), mimetype="text/csv")
    if download_filename is not None:
        response.headers[
            "Content-Disposition"
        ] = f'attachment; filename="{download_filename}"'
    return response


def make_xlsx_response(
    xlsx_buf: io.BytesIO, download_filename: Optional[str] = None
) -> Response:
    def generate():
        minibuf = xlsx_buf.read(4096)
        while minibuf:
            yield minibuf
            minibuf = xlsx_buf.read(4096)

    excel_mimetype = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"

    response = current_app.response_class(generate(), mimetype=excel_mimetype)
    if download_filename is not None:
        response.headers[
            "Content-Disposition"
        ] = f'attachment; filename="{download_filename}"'
    return response


def is_browser():
    # bit of content negotiation magic
    accepts = werkzeug.http.parse_accept_header(request.headers.get("Accept"))
    best = accepts.best_match(["text/html", "text/csv"], default="text/csv")
    return best == "text/html"


def byte_buf_to_str_buf(byte_buf):
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
    g.user_uuid = user.user_uuid
    g.username = user.username

    if session is None:
        session = flask_session
    session["user_uuid"] = user.user_uuid
    # Make it last for 31 days
    session.permanent = True


def set_current_user(user: User):
    g.username = user.username
    g.user_uuid = user.user_uuid


def json_or_400() -> Dict[str, Any]:
    if request.json is None:
        abort(400)
    else:
        return request.json


def get_sesh() -> Session:
    return current_app.scoped_session  # type: ignore


def snake_case(inp: str) -> str:
    # FIXME: this ignores capitalisations...
    return inp.replace("-", "_")
