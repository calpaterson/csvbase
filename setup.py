from setuptools import setup, find_packages

VERSION = open("csvbase/VERSION").read().strip()

reqs = open("requirements.txt").read().strip().split("\n")

test_reqs = open("requirements-test.txt").read().strip().split("\n")

setup(
    name="csvbase",
    version=VERSION,
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    package_data={"csvbase": ["py.typed"]},
    zip_safe=False,
    install_requires=reqs,
    extras_require={"tests": test_reqs},
    entry_points={
        "console_scripts": [
            "csvbase-make-tables=csvbase.cli:make_tables",
            "csvbase-make-blog-table=csvbase.web.blog.cli:make_blog_table",
            "csvbase-load-prohibited-usernames=csvbase.cli:load_prohibited_usernames",
            "csvbase-config=csvbase.cli:config_cli",
            "csvbase-update-stripe-subscriptions=csvbase.cli:update_stripe_subscriptions",
            "csvbase-update-external-tables=csvbase.cli:update_external_tables",
            "csvbase-repcache-populate=csvbase.cli:repcache_populate",
        ]
    },
)
