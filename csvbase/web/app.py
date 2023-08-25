import json
import secrets
from datetime import timedelta
from logging import getLogger
from typing import (
    Any,
    Dict,
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
    g,
    jsonify,
    make_response,
    redirect,
    request,
    url_for,
)
from flask import session as flask_session
from flask.wrappers import Response as FlaskResponse
from flask_babel import Babel
from passlib.context import CryptContext
from werkzeug.routing import BaseConverter
from werkzeug.wrappers.response import Response

from .func import set_current_user, user_timezone_or_utc, format_timedelta
from .. import exc, svc
from .blog.bp import bp as blog_bp
from ..config import get_config
from ..db import db, get_db_url
from ..logging import configure_logging
from .. import sentry
from ..sesh import get_sesh
from .func import is_browser
from .billing import bp as billing_bp
from .main.bp import bp as main_bp

logger = getLogger(__name__)

CORS_EXPIRY = timedelta(hours=8)

EXCEPTION_MESSAGE_CODE_MAP = {
    exc.UserDoesNotExistException: ("that user does not exist", 404),
    exc.RowDoesNotExistException: ("that row does not exist", 404),
    exc.PageDoesNotExistException: ("that page does not exist", 404),
    exc.TableDoesNotExistException: ("that table does not exist", 404),
    exc.NotAuthenticatedException: ("you need to sign in to do that", 401),
    exc.NotAllowedException: ("that's not allowed", 403),
    exc.WrongAuthException: ("that's the wrong password or api key", 400),
    exc.InvalidAPIKeyException: ("invalid api key", 400),
    exc.InvalidRequest: ("invalid request", 400),
    exc.CantNegotiateContentType: ("can't agree with you on a content type", 406),
    exc.WrongContentType: ("you sent the wrong content type", 400),
    exc.ProhibitedUsernameException: ("that username is not allowed", 400),
    exc.UsernameAlreadyExistsException: ("that username is taken", 400),
    exc.UsernameAlreadyExistsInDifferentCaseException: (
        "that username is taken (in a different case)",
        400,
    ),
    exc.CSVException: ("Unable to parse that csv file", 400),
    exc.UnknownPaymentReferenceUUIDException: ("unknown payment reference", 404),
    exc.NotEnoughQuotaException: ("this would exceed your quota", 400),
    exc.InvalidTableNameException: ("that table name is invalid", 400),
    exc.InvalidUsernameNameException: ("that username is invalid", 400),
}


def init_app() -> Flask:
    configure_logging()
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

    app.url_map.converters["table_name"] = TableNameConverter

    app.register_blueprint(main_bp)

    billing_bp.init_blueprint(app)

    if config.blog_ref is not None:
        app.register_blueprint(blog_bp)

    @app.context_processor
    def inject_blueprints() -> Dict:
        return dict(blueprints=app.blueprints.keys())

    app.jinja_env.filters["snake_case"] = snake_case
    app.jinja_env.filters["ppjson"] = ppjson
    app.jinja_env.filters["timedeltaformat"] = format_timedelta

    app.config["SQLALCHEMY_DATABASE_URI"] = get_db_url()
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)

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


def snake_case(inp: str) -> str:
    # FIXME: this ignores capitalisations...
    return inp.replace("-", "_")


def ppjson(inp: Union[Mapping, Sequence]) -> str:
    return json.dumps(inp, indent=4)
