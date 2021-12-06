from uuid import UUID
import io
import shutil
import codecs
from logging import basicConfig, INFO, getLogger
from typing import Optional, Any

from cchardet import UniversalDetector
from flask import (
    g,
    session as flask_session,
    Flask,
    request,
    abort,
    make_response,
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
from sqlalchemy.orm import sessionmaker
from flask_sqlalchemy_session import flask_scoped_session
import werkzeug.http

from . import svc
from . import db


def init_app():
    basicConfig(level=INFO)
    app = Flask(__name__)
    app.config["CRYPT_CONTEXT"] = CryptContext(["argon2"])
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_NAME"] = "csvbase_websesh"
    app.config["SECRET_KEY"] = "no peeking"

    app.register_blueprint(bp)

    db.make_tables()

    sesh = flask_scoped_session(sessionmaker(bind=db.engine))
    sesh.init_app(app)

    return app


logger = getLogger(__name__)

bp = Blueprint("csvbase", __name__)


@bp.before_request
def put_user_in_g() -> None:
    app_logger = current_app.logger
    user_uuid: Optional[Any] = flask_session.get("user_uuid")
    if user_uuid is not None:
        if not isinstance(user_uuid, UUID):
            del flask_session["user_uuid"]
            app_logger.warning("cleared a corrupt user_uuid cookie: %s", user_uuid)
        else:
            sesh = current_app.scoped_session
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
def index_redirect():
    return redirect(url_for("csvbase.paste"))


@bp.route("/paste")
def paste():
    return make_response(render_template("paste.html"))


@bp.route("/<username>/<table_name>", methods=["GET"])
def get_table(username, table_name):
    sesh = current_app.scoped_session
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user_uuid = svc.user_uuid_for_name(sesh, username)
    if is_browser():
        cols = svc.get_columns(sesh, username, table_name, include_row_id=True)
        row_iter = svc.table_as_rows(sesh, user_uuid, username, table_name)
        return make_response(
            render_template(
                "table.html",
                cols=cols,
                row_iter=row_iter,
                username=username,
                table_name=table_name,
            )
        )
    else:
        return make_csv_response(
            svc.table_as_csv(sesh, user_uuid, username, table_name)
        )


@bp.route("/<username>/<table_name>/rows/<int:row_id>", methods=["GET"])
def get_row(username, table_name, row_id):
    sesh = current_app.scoped_session
    user_uuid = svc.user_uuid_for_name(sesh, username)
    row = svc.get_row(sesh, username, table_name, row_id)
    if is_browser():
        return make_response(
            render_template(
                "row.html",
                row=row,
                row_id=row_id,
                username=username,
                table_name=table_name,
            )
        )
    else:
        return jsonify(
            {
                "row_id": row_id,
                "row": {
                    column.name: column.value_to_json(value)
                    for column, value in row.items()
                },
            }
        )


@bp.route("/<username>/<table_name>/rows/<int:row_id>", methods=["PUT"])
def update_row(username, table_name, row_id):
    abort(501)


@bp.route("/<username>/<table_name>/rows/<int:row_id>/edit", methods=["POST"])
def update_row_by_form_post(username, table_name, row_id):
    sesh = current_app.scoped_session
    columns = svc.get_columns(sesh, username, table_name)
    values = {c.name: c.python_type(request.form[c.name]) for c in columns}
    svc.update_row(sesh, username, table_name, row_id, values)
    sesh.commit()
    flash(f"Updated row {row_id}")
    return redirect(
        url_for(
            "csvbase.get_row", username=username, table_name=table_name, row_id=row_id
        )
    )


@bp.route("/new-table", methods=["POST"])
def new_table_form_submission():
    sesh = current_app.scoped_session
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
    svc.upsert_table(sesh, g.user_uuid, g.username, table_name, csv_buf)
    sesh.commit()
    return redirect(
        url_for("csvbase.get_table", username=g.username, table_name=table_name)
    )


# FIXME: assert table name and user name match regex
@bp.route("/<username>/<table_name>", methods=["PUT"])
def upsert_table(username, table_name):
    sesh = current_app.scoped_session
    am_user_or_400(username)
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
    sesh = current_app.scoped_session
    tables = svc.tables_for_user(sesh, svc.user_uuid_for_name(sesh, username))
    return make_response(
        render_template(
            "user.html",
            username=username,
            table_names=tables,
        )
    )


@bp.route("/sign-in", methods=["GET", "POST"])
def sign_in():
    sesh = current_app.scoped_session
    if request.method == "GET":
        return make_response(
            render_template(
                "sign_in.html",
            )
        )
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
            return redirect(url_for("csvbase.user", username=request.form["username"]))
        else:
            abort(400)


@bp.route("/sign-out", methods=["GET"])
def sign_out():
    flask_session.clear()
    flash("Signed out")
    return redirect(url_for("csvbase.paste"))


def am_user_or_400(username):
    # hardcoded user and pass for now
    auth = request.authorization
    if auth is not None:
        if auth.username == "calpaterson" and auth.password == "password":
            return
    else:
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
