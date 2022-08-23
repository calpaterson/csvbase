import pkg_resources


def get_version() -> str:
    version = (
        pkg_resources.resource_string("csvbase", "VERSION").decode("utf-8").strip()
    )
    return version
