from uuid import UUID
import io
import shutil
import codecs
from logging import basicConfig, INFO, getLogger

from cchardet import UniversalDetector
from flask import (
    Flask,
    request,
    abort,
    make_response,
    make_response,
    render_template,
    redirect,
    url_for,
)
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from flask_sqlalchemy_session import flask_scoped_session
import werkzeug.http

from . import svc

app = Flask(__name__)

engine = create_engine("postgresql:///csvbase")
sesh = flask_scoped_session(sessionmaker(bind=engine))

logger = getLogger(__name__)


@app.before_first_request
def lower_logging_level():
    basicConfig(level=INFO)


@app.route("/")
def landing():
    return make_response(render_template("paste.html"))


@app.route("/<username>/<table_name>", methods=["GET"])
def get_table(username, table_name):
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    user_uuid = svc.user_uuid_for_name(sesh, username)
    if is_browser():
        cols = svc.get_columns(sesh, username, table_name)
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


@app.route("/new-table", methods=["POST"])
def new_table_form_submission():
    # FIXME: require a login
    # am_a_user()
    user_uuid, username = UUID("ffeb73b9-914b-4ede-9fc3-965e0fc1a556"), "calpaterson"
    table_name = request.form["table-name"]
    textarea = request.form.get("csv-textarea")
    if textarea:
        csv_buf = io.StringIO(textarea)
    else:
        csv_buf = byte_buf_to_str_buf(request.files["csv-file"])
    svc.upsert_table(
        sesh, svc.user_uuid_for_name(sesh, username), username, table_name, csv_buf
    )
    sesh.commit()
    return redirect(url_for("get_table", username=username, table_name=table_name))


# FIXME: assert table name and user name match regex
@app.route("/<username>/<table_name>", methods=["PUT"])
def upsert_table(username, table_name):
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


@app.route("/<username>")
def user(username):
    ...


@app.route("/sign-up")
def post():
    ...


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

    return app.response_class(generate(), mimetype="text/csv")


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
