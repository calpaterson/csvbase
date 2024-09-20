from logging import getLogger
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

import toml

logger = getLogger(__name__)


@dataclass
class Config:
    """A typecheckable config object.

    In order to keep the benefits of typechecking, don't pass this into places
    where the typechecker can't find it - ie: jinja templates.

    """

    db_url: str
    environment: str
    blog_ref: Optional[str]
    secret_key: Optional[str]
    sentry_dsn: Optional[str]
    stripe_api_key: Optional[str]
    stripe_price_id: Optional[str]
    enable_datadog: bool
    x_accel_redirect: bool
    smtp_host: Optional[str]
    memcache_server: Optional[str]

    # configuration for Cloudflare turnstile (a captcha tool)
    turnstile_site_key: Optional[str]
    turnstile_secret_key: Optional[str]

    celery_broker_url: Optional[str] = "redis://localhost/3"


__config__: Optional[Config] = None


def load_config(config_file: Path) -> Config:
    """Loads the configuration at the given path.

    Currently this doesn't really validate the config - but that is planned.

    """
    logger.info("loading config from %s", config_file)
    if config_file.exists():
        with open(config_file, encoding="utf-8") as config_f:
            as_dict = toml.load(config_f)
    else:
        logger.warning("config file ('%s') not found, using defaults", config_file)
        as_dict = {}
    return Config(
        db_url=as_dict.get("db_url", "postgresql:///csvbase"),
        environment=as_dict.get("environment", "local"),
        blog_ref=as_dict.get("blog_ref"),
        secret_key=as_dict.get("secret_key"),
        sentry_dsn=as_dict.get("sentry_dsn"),
        stripe_price_id=as_dict.get("stripe_price_id"),
        stripe_api_key=as_dict.get("stripe_api_key"),
        enable_datadog=as_dict.get("enable_datadog", False),
        x_accel_redirect=as_dict.get("x_accel_redirect", False),
        turnstile_site_key=as_dict.get("turnstile_site_key"),
        turnstile_secret_key=as_dict.get("turnstile_secret_key"),
        smtp_host=as_dict.get("smtp_host"),
        memcache_server=as_dict.get("memcache_server"),
    )


def default_config_file() -> Path:
    """Returns the location of the default config file"""
    return Path.home() / ".csvbase.toml"


def get_config() -> Config:
    """Returns the config.

    The the config does not change while the program is running, but in order
    to make it easy to test, don't call this function from the top-level (that
    makes it hard to mock).

    """
    global __config__
    if __config__ is None:
        __config__ = load_config(default_config_file())

    return __config__
