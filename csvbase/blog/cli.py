from . import svc

import click


@click.command()
def make_blog_table():
    from csvbase.web import get_sesh, init_app

    with init_app().app_context():
        sesh = get_sesh()
        svc.make_blog_table(sesh)
        sesh.commit()
