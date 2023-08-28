from setuptools import setup, find_packages

VERSION = open("csvbase/VERSION").read().strip()

# The tests test the blog, so it must be installed
test_reqs = [
    "bandit==1.7.4",
    "black==22.3.0",
    "bpython~=0.22.1",
    "feedparser==6.0.2",
    "mypy==0.982",
    "openpyxl==3.1.2",
    "pandas==1.3.5",
    "pytest-flask==1.2.0",
    "pytest==7.1.1",
    "sqlalchemy-stubs==0.4",
    "types-bleach==5.0.2",
    "types-setuptools==65.1.0",
    "types-toml==0.10.8.5",
]

setup(
    name="csvbase",
    version=VERSION,
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    zip_safe=False,
    install_requires=[
        "alembic[tz]==1.7.7",
        "argon2-cffi==21.3.0",
        "bleach==6.0.0",
        "cchardet==2.1.7",
        "click==8.1.3",
        "python-dateutil==2.8.2",
        "feedgen==0.9.0",
        "flask-babel==3.1.0",
        "flask-cors==3.0.10",
        "flask-sqlalchemy==3.0.3",
        "flask==2.3.3",
        "gunicorn==20.1.0",
        "inflect==6.0.4",
        "marko[codehilite]==1.3.0",
        "passlib==1.7.4",
        "pgcopy==1.5.0",
        "psycopg2==2.9.6",
        "pyarrow==11.0.0",
        "sentry-sdk[flask]==1.29.2",
        "sqlalchemy==1.4.47",
        "stripe==5.4.0",
        "systemd-python==235",
        "toml==0.10.2",
        "typing-extensions==4.5.0",
        "werkzeug==2.3.7",
        "xlsxwriter==3.0.9",
    ],
    extras_require={"tests": test_reqs},
    entry_points={
        "console_scripts": [
            "csvbase-make-tables=csvbase.cli:make_tables",
            "csvbase-make-blog-table=csvbase.web.blog.cli:make_blog_table",
            "csvbase-load-prohibited-usernames=csvbase.cli:load_prohibited_usernames",
            "csvbase-config=csvbase.cli:config_cli",
            "csvbase-update-stripe-subscriptions=csvbase.cli:update_stripe_subscriptions",
        ]
    },
)
