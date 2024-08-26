import json
import secrets
from datetime import timedelta
from logging import getLogger
from typing import (
    Any,
    Mapping,
    Optional,
    Sequence,
    Union,
)
from uuid import UUID

from flask import (
    Flask,
    current_app,
    flash,
    jsonify,
    make_response,
    redirect,
    request,
    url_for,
)
from flask import session as flask_session, render_template
from flask.wrappers import Response as FlaskResponse
from flask_babel import Babel
from passlib.context import CryptContext
from werkzeug.routing import BaseConverter
from werkzeug.wrappers.response import Response

from .func import (
    set_current_user,
    user_timezone_or_utc,
    format_timedelta,
    handle_app_level_404_and_405,
)
from .. import exc, svc
from . import schemaorg
from .blog.bp import bp as blog_bp
from .faq.bp import bp as faq_bp
from ..config import get_config
from ..db import db, get_db_url
from ..logging import configure_logging
from .. import sentry, datadog
from ..sesh import get_sesh
from ..markdown import render_markdown
from .func import is_browser, is_url, get_current_user
from .billing import bp as billing_bp
from .main.bp import bp as main_bp
from .main.create_table import bp as create_table_bp
from ..value_objs import ContentType, ROW_ID_COLUMN
from ..bgwork.core import initialise_celery


logger = getLogger(__name__)

CORS_EXPIRY = timedelta(hours=8)

EXCEPTION_MESSAGE_CODE_MAP = {
    exc.UserDoesNotExistException: ("that user does not exist", 404),
    exc.RowDoesNotExistException: ("that row does not exist", 404),
    exc.PageDoesNotExistException: ("that page does not exist", 404),
    exc.TableDoesNotExistException: ("that table does not exist", 404),
    exc.TableUUIDDoesNotExistException: ("that table does not exist", 404),
    exc.NotAuthenticatedException: ("you need to sign in to do that", 401),
    exc.NotAllowedException: ("that's not allowed", 403),
    exc.WrongAuthException: ("that's the wrong password or api key", 400),
    exc.InvalidAPIKeyException: ("invalid api key", 400),
    exc.InvalidRequest: ("invalid request", 400),
    exc.CantNegotiateContentType: ("can't agree with you on a content type", 406),
    exc.TooBigForContentType: ("table too big for that content type", 406),
    exc.WrongContentType: ("you sent the wrong content type", 400),
    exc.ProhibitedUsernameException: ("that username is not allowed", 400),
    exc.UsernameAlreadyExistsException: ("that username is taken", 400),
    exc.UsernameAlreadyExistsInDifferentCaseException: (
        "that username is taken (in a different case)",
        400,
    ),
    exc.CSVParseError: ("unable to parse that csv file", 400),
    exc.UnknownPaymentReferenceUUIDException: ("unknown payment reference", 404),
    exc.NotEnoughQuotaException: ("this would exceed your quota", 400),
    exc.InvalidTableNameException: ("that table name is invalid", 400),
    exc.InvalidUsernameNameException: ("that username is invalid", 400),
    exc.TableDefinitionMismatchException: (
        "columns or types don't match existing",
        400,
    ),
    exc.WrongEncodingException: ("you sent a file with the wrong encoding", 400),
    exc.ETagMismatch: ("you provided an ETag different to the current one", 412),
    exc.MissingTempFile: (
        "the temp file you're working on is missing - perhaps you took too long?",
        400,
    ),
    exc.UnconvertableValueException: (
        "unable to convert the data you provided to the required type",
        422,
    ),
    exc.ReadOnlyException: ("that table is read-only", 400),
    exc.FAQEntryDoesNotExistException: ("that FAQ entry does not exist", 404),
    exc.TableAlreadyExists: ("that table already exists", 409),
    exc.CaptchaFailureException: ("you failed the captcha", 403),
}


def init_app() -> Flask:
    configure_logging()
    datadog.configure_datadog()
    sentry.configure_sentry()
    app = Flask(__name__)
    app.config["CRYPT_CONTEXT"] = CryptContext(["argon2"])
    app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
    app.config["SESSION_COOKIE_NAME"] = "csvbase_websesh"
    app.config["SESSION_REFRESH_EACH_REQUEST"] = False
    app.json.compact = False  # type: ignore
    config = get_config()
    if config.secret_key is not None:
        app.config["SECRET_KEY"] = config.secret_key
    else:
        app.logger.warning("CSVBASE_SECRET_KEY not set, using a random secret")
        app.config["SECRET_KEY"] = secrets.token_hex()

    Babel(
        app,
        default_locale="en_GB",
        default_timezone="Europe/London",
        timezone_selector=user_timezone_or_utc,
    )

    class TableNameConverter(BaseConverter):
        # FIXME: Not sure it is a good idea to reject invalid table names on a
        # url-map level
        regex = r"[A-Za-z][-A-Za-z0-9]+"

    class UsernameConverter(BaseConverter):
        # However it is certainly a good idea to reject invalid usernames on a
        # url-map level, else incorrect urls get "this user does not exist"
        regex = r"^[A-Za-z][-A-Za-z0-9]+"

    app.url_map.converters["table_name"] = TableNameConverter
    app.url_map.converters["username"] = UsernameConverter

    app.register_blueprint(main_bp)
    app.register_blueprint(create_table_bp)

    billing_bp.init_blueprint(app)

    if config.blog_ref is not None:
        app.register_blueprint(blog_bp)
    app.register_blueprint(faq_bp)

    app.jinja_env.globals["is_url"] = is_url
    app.jinja_env.globals["ROW_ID_COLUMN"] = ROW_ID_COLUMN
    app.jinja_env.globals["blueprints"] = app.blueprints.keys()
    app.jinja_env.globals["ContentType"] = ContentType
    app.jinja_env.globals["schemaorg"] = schemaorg

    if config.turnstile_site_key is not None:
        app.jinja_env.globals["turnstile_site_key"] = config.turnstile_site_key

    app.jinja_env.filters["snake_case"] = snake_case
    app.jinja_env.filters["ppjson"] = ppjson
    app.jinja_env.filters["timedeltaformat"] = format_timedelta
    app.jinja_env.filters["render_markdown"] = render_markdown

    @app.context_processor
    def inject_user():
        current_user = get_current_user()
        return (
            {
                "current_user": current_user,
                "current_username": current_user.username,
            }
            if current_user is not None
            else {}
        )

    app.jinja_env.add_extension("jinja2_humanize_extension.HumanizeExtension")

    app.config["SQLALCHEMY_DATABASE_URI"] = get_db_url()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)
    initialise_celery(app, config)

    # Currently the toolbar is broken (and I wouldn't want to enable it by
    # default anyway - too dangerous) but it can be used if you downgrade to
    # 'flask-sqlalchemy<3'
    # from flask_debugtoolbar import DebugToolbarExtension
    # DebugToolbarExtension(app)

    # typing for errorhandler is apparently tricky...
    # https://github.com/pallets/flask/blob/bd56d19b167822a9a23e2e9e2a07ccccc36baa8d/src/flask/typing.py#L49
    @app.errorhandler(exc.CSVBaseException)
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
            elif isinstance(e, exc.NotEnoughQuotaException):
                flash("You need to subscribe in order to do that")
                return redirect(url_for("billing.pricing"))
            else:
                resp = make_response(
                    render_template(
                        "error-dynamic.html", http_code=http_code, message=message
                    )
                )
                resp.status_code = http_code
                return resp
        else:
            if isinstance(e, exc.CSVParseError):
                doc = {"error": message, "detail": e.message}
            else:
                doc = {"error": message}
            resp = jsonify(doc)
            resp.status_code = http_code
            return resp

    app.register_error_handler(404, handle_app_level_404_and_405)
    app.register_error_handler(405, handle_app_level_404_and_405)

    @app.before_request
    def put_user_on_request() -> None:
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
                    current_user = svc.user_by_user_uuid(sesh, user_uuid)
                except exc.UserDoesNotExistException:
                    del flask_session["user_uuid"]
                    app_logger.warning(
                        "cleared a corrupt user_uuid cookie: %s", user_uuid
                    )
                else:
                    set_current_user(current_user)
                    app_logger.debug(
                        "currently signed in as: %s", current_user.username
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


def snake_case(inp: str) -> str:
    # FIXME: this ignores capitalisations...
    return inp.replace("-", "_")


def ppjson(inp: Union[Mapping, Sequence]) -> str:
    return json.dumps(inp, indent=4)
