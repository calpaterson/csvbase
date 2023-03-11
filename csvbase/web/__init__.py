# this is necessary for legacy reasons - init_app was moved and gunicorn and
# others still look for it in this namespace
from .app import init_app
