from pathlib import Path
import toml

from csvbase.config import load_config, Config


def test_config_file_not_exist(tmpdir):
    config = load_config(Path(tmpdir / "config.toml"))
    assert config == Config(
        db_url="postgresql:///csvbase",
        environment="local",
        blog_ref=None,
        secret_key=None,
        sentry_dsn=None,
        stripe_api_key=None,
        stripe_price_id=None,
        enable_datadog=False,
        x_accel_redirect=False,
    )


def test_config_file_basic(tmpdir):
    config = {"db_url": "postgresql:///csvboth", "environment": "test"}
    config_file = Path(tmpdir / "config.toml")
    with open(config_file, "w") as config_f:
        toml.dump(config, config_f)

    assert load_config(config_file) == Config(
        db_url="postgresql:///csvboth",
        environment="test",
        blog_ref=None,
        secret_key=None,
        sentry_dsn=None,
        stripe_api_key=None,
        stripe_price_id=None,
        enable_datadog=False,
        x_accel_redirect=False,
    )
