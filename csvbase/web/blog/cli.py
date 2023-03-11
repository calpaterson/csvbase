from . import svc

import click

from csvbase.web.app import init_app
from csvbase.sesh import get_sesh


@click.command()
def make_blog_table():
    with init_app().app_context():
        sesh = get_sesh()
        svc.make_blog_table(sesh)
        sesh.commit()
