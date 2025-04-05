FROM python:3.9-slim-bullseye
WORKDIR /app
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get -y install libpq5 libpq-dev python3-dev libsystemd-dev build-essential pkg-config curl unzip

COPY requirements.txt ./
RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -r requirements.txt

COPY . .

RUN pygmentize -S default -f html -a .highlight > csvbase/web/static/codehilite.css
RUN pygmentize -S lightbulb -f html -a .highlight > csvbase/web/static/codehilite-dark.css
RUN make static-deps

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install -e .

ENV FLASK_APP=csvbase.web.app:init_app()
ENV FLASK_DEBUG=0
EXPOSE 6001
CMD ["gunicorn", "csvbase.web.app:init_app()", "-b", ":6001"]

