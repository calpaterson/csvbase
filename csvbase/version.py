import importlib.resources


def get_version() -> str:
    version = importlib.resources.read_text("csvbase", "VERSION").strip()
    return version
