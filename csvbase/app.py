import io
import shutil
import codecs
from logging import basicConfig, INFO

from flask import Flask, request, abort, make_response
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from flask_sqlalchemy_session import flask_scoped_session

from . import svc

app = Flask(__name__)

engine = create_engine("postgresql:///csvbase")
sesh = flask_scoped_session(sessionmaker(bind=engine))


@app.before_first_request
def lower_logging_level():
    basicConfig(level=INFO)


@app.route("/")
def landing():
    return "<p>Hello, World!</p>"


@app.route("/<username>/<table_name>", methods=["GET"])
def get_table(username, table_name):
    svc.is_public(sesh, username, table_name) or am_user_or_400(username)
    return make_csv_response(
        svc.get_table(
            sesh, svc.user_uuid_for_name(sesh, username), username, table_name
        )
    )


# FIXME: assert table name and user name match regex
@app.route("/<username>/<table_name>", methods=["PUT"])
def upsert_table(username, table_name):
    am_user_or_400(username)
    # FIXME: add checking for forms here
    buf = io.StringIO()
    # FIXME: assuming utf-8, unlikely
    in_buf = codecs.getreader("utf-8")(request.stream)
    shutil.copyfileobj(in_buf, buf)
    buf.seek(0)
    svc.upsert_table(
        sesh, svc.user_uuid_for_name(sesh, username), username, table_name, buf
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
