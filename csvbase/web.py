from uuid import UUID
import io
import shutil
import codecs
from logging import basicConfig, INFO, getLogger
from typing import Optional, Any, Dict, List, Tuple, NoReturn
from os import environ

from typing_extensions import Literal
from cchardet import UniversalDetector
from werkzeug.wrappers.response import Response
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

from .value_objs import KeySet, ColumnType, PythonType, Column
from . import svc
from . import db
from . import exc


def init_app():
    basicConfig(level=INFO)
    app = Flask(__name__)
    app.config["CRYPT_CONTEXT"] = CryptContext(["argon2"])
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_NAME"] = "csvbase_websesh"
    app.config["SECRET_KEY"] = "no peeking"

    app.register_blueprint(bp)

    db.make_tables()

    @app.context_processor
    def inject_heroku():
        return dict(HEROKU="HEROKU" in environ)

    sesh = flask_scoped_session(sessionmaker(bind=db.engine))
    sesh.init_app(app)

    return app


logger = getLogger(__name__)

bp = Blueprint("csvbase", __name__)


@bp.errorhandler(exc.TableDoesNotExistException)
def handle_table_does_not_exist(e):
    message = "table does not exist"
    http_code = 404
    if is_browser():
        return f"{http_code}: {message}", http_code
    else:
        return jsonify({"error": message}), http_code


@bp.errorhandler(exc.UserDoesNotExistException)
def handle_user_does_not_exist(e):
    message = "user does not exist"
    http_code = 404
    if is_browser():
        return f"{http_code}: {message}", http_code
    else:
        return jsonify({"error": message}), http_code


@bp.errorhandler(exc.RowDoesNotExistException)
def handle_row_does_not_exist(e):
    message = "row does not exist"
    http_code = 404
    if is_browser():
        return f"{http_code}: {message}", http_code
    else:
        return jsonify({"error": message}), http_code


@bp.before_request
def put_user_in_g() -> None:
    app_logger = current_app.logger
    user_uuid: Optional[Any] = flask_session.get("user_uuid")
    if user_uuid is not None:
        if not isinstance(user_uuid, UUID):
            del flask_session["user_uuid"]
            app_logger.warning("cleared a corrupt user_uuid cookie: %s", user_uuid)
        else:
            sesh = get_sesh()
            username = svc.username_from_user_uuid(sesh, user_uuid)
            if username is None:
                del flask_session["user_uuid"]
                app_logger.warning("cleared a corrupt user_uuid cookie: %s", user_uuid)
            else:
                set_current_user(username, user_uuid)
                app_logger.debug("currently signed in as: %s", g.username)
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
        user_uuid = svc.create_user(
            sesh,
            current_app.config["CRYPT_CONTEXT"],
            request.form["username"],
            request.form.get("email"),
            request.form["password"],
        )
        set_current_user_for_session(request.form["username"], user_uuid)
        flash("Account created")
    else:
        am_a_user()

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
    am_a_user()
    sesh = get_sesh()
    am_user_or_400(sesh, g.username)
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


@bp.route("/<username>/<table_name>", methods=["GET"])
def get_table(username: str, table_name: str) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(sesh, username)
    user_uuid = svc.user_uuid_for_name(sesh, username)

    # passing a default and type here means the default is used if what they
    # provide can't be parsed
    n: int = request.args.get("n", default=1, type=int)
    op: Literal["greater_than", "less_than"] = (
        "greater_than" if request.args.get("op", default="gt") == "gt" else "less_than"
    )
    keyset = KeySet(n=n, op=op)

    if is_browser():
        cols = svc.get_columns(sesh, username, table_name, include_row_id=True)
        page = svc.table_page(sesh, user_uuid, username, table_name, keyset)
        return make_response(
            render_template(
                "table.html",
                cols=cols,
                page=page,
                keyset=keyset,
                username=username,
                table_name=table_name,
            )
        )
    else:
        return make_csv_response(
            svc.table_as_csv(sesh, user_uuid, username, table_name)
        )


@bp.route("/<username>/<table_name>/rows/<int:row_id>", methods=["GET"])
def get_row(username: str, table_name: str, row_id: int) -> Tuple[Response, int]:
    sesh = get_sesh()
    svc.user_exists(sesh, username)
    if not svc.is_public(sesh, username, table_name) and not am_user(sesh, username):
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


@bp.route("/<username>/<table_name>/rows/<int:row_id>", methods=["PUT"])
def update_row(username: str, table_name: str, row_id: int) -> Response:
    sesh = get_sesh()
    svc.is_public(sesh, username, table_name) or am_user_or_400(sesh, username)
    body: Dict[str, Any] = json_or_400()
    assert body["row_id"] == row_id, "row ids cannot be changed"
    svc.update_row(sesh, username, table_name, row_id, body["row"])
    sesh.commit()
    return jsonify({})


@bp.route("/<username>/<table_name>/rows/<int:row_id>/edit", methods=["POST"])
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
@bp.route("/<username>/<table_name>", methods=["PUT"])
def upsert_table(username, table_name):
    sesh = get_sesh()
    am_user_or_400(sesh, username)
    # FIXME: add checking for forms here
    byte_buf = io.BytesIO()
    shutil.copyfileobj(request.stream, byte_buf)
    str_buf = byte_buf_to_str_buf(byte_buf)
    svc.upsert_table(
        sesh, svc.user_uuid_for_name(sesh, username), username, table_name, str_buf
    )
    sesh.commit()
    return make_text_response(f"upserted {username}/{table_name}")


@bp.route("/<username>", methods=["GET"])
def user(username):
    sesh = get_sesh()
    tables = svc.tables_for_user(
        sesh,
        svc.user_uuid_for_name(sesh, username),
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
def sign_in():
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
                username,
                svc.user_uuid_for_name(sesh, request.form["username"]),
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
            abort(400)


@bp.route("/sign-out", methods=["GET"])
def sign_out():
    flask_session.clear()
    flash("Signed out")
    if request.referrer:
        return redirect(request.referrer)
    else:
        return redirect(url_for("csvbase.paste"))


def am_user(sesh: Session, username: str) -> bool:
    """Return true if the current user has the given username.

    This is ascertained by first checking cookies, then basic auth.

    """
    if "username" in g:
        return g.username == username
    auth = request.authorization
    if auth is not None:
        if svc.is_correct_password(
            sesh, current_app.config["CRYPT_CONTEXT"], auth.username, auth.password
        ):
            return True
    return False


def am_user_or_400(sesh: Session, username: str) -> bool:
    if not am_user(sesh, username):
        abort(400)
    return True


def am_a_user():
    if not g.username and g.user_uuid:
        abort(400)


def make_text_response(text: str, status=200):
    # need a trailing newline else shells get confused
    resp = make_response("200 OK: " + text + "\n")
    resp.headers["Content-Type"] = "text/plain"
    return resp


def make_csv_response(csv_buf, status=200):
    def generate():
        minibuf = csv_buf.read(4096)
        while minibuf:
            yield minibuf
            minibuf = csv_buf.read(4096)

    return current_app.response_class(generate(), mimetype="text/csv")


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


def set_current_user_for_session(username, user_uuid, session: Optional[Any] = None):
    """Sets the current user and creates a web session."""
    g.user_uuid = user_uuid
    g.username = username

    if session is None:
        session = flask_session
    session["user_uuid"] = user_uuid
    # Make it last for 31 days
    session.permanent = True


def set_current_user(username, user_uuid):
    g.username = username
    g.user_uuid = user_uuid


def json_or_400() -> Dict[str, Any]:
    if request.json is None:
        abort(400)
    else:
        return request.json


def get_sesh() -> Session:
    return current_app.scoped_session  # type: ignore
