from logging import getLogger
from pathlib import Path
from typing import Optional
from dataclasses import dataclass

import toml

logger = getLogger(__name__)


@dataclass
class Config:
    db_url: str
    environment: str
    blog_ref: Optional[str]
    secret_key: Optional[str]
    sentry_dsn: Optional[str]


__config__: Optional[Config] = None


def load_config(config_file: Path) -> Config:
    """Loads the configuration at the given path"""
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
    )


def default_config_file() -> Path:
    """Returns the location of the default config file"""
    return Path.home() / ".csvbase.toml"


def get_config() -> Config:
    """Returns the config"""
    global __config__
    if __config__ is None:
        __config__ = load_config(default_config_file())

    return __config__
