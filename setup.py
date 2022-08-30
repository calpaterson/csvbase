from setuptools import setup, find_packages

VERSION = open("csvbase/VERSION").read().strip()

setup(
    name="csvbase",
    version=VERSION,
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "alembic[tz]==1.7.7",
        "argon2-cffi==21.1.0",
        "bleach==5.0.0",
        "cchardet==2.1.7",
        "flask-cors==3.0.10",
        "flask-sqlalchemy-session==1.1",
        "flask-babel==2.0",
        "flask==2.0.2",
        "gunicorn==20.1.0",
        "inflect==5.6.0",
        "marko==1.2.0",
        "passlib==1.7.4",
        "pgcopy==1.5.0",
        "psycopg2==2.9.2",
        "sentry-sdk[flask]==1.9.5",
        "sqlalchemy==1.4.27",
        "systemd-python==234",
        "typing-extensions==4.0.1",
        # werkzeug 2.1.0 broke flask-sqlalchemy-session:
        # https://github.com/dtheodor/flask-sqlalchemy-session/issues/14
        "werkzeug<2.1.0",
        "xlsxwriter==3.0.3",
    ],
    extras_require={
        "tests": [
            "black==22.3.0",
            "bpython~=0.22.1",
            "mypy==0.910",
            "pandas==1.3.5",
            "pytest-flask==1.2.0",
            "pytest==7.1.1",
            "sqlalchemy-stubs==0.4",
            "bandit==1.6.3",
            "types-bleach==5.0.2",
            "types-setuptools==65.1.0",
        ],
    },
    entry_points={
        "console_scripts": [
            "csvbase-make-tables=csvbase.cli:make_tables",
            "csvbase-load-prohibited-usernames=csvbase.cli:load_prohibited_usernames",
        ]
    },
)
