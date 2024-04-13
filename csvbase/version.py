from contextlib import closing

import importlib_resources


def get_version() -> str:
    with closing(
        importlib_resources.files("csvbase").joinpath("VERSION").open("r")
    ) as text_f:
        version = text_f.read().strip()
        return version
