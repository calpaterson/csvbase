# syntax=docker/dockerfile:1.4
FROM python:3.9-slim-bullseye as builder
WORKDIR /app
RUN rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' > /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && \
    apt-get install -y libpq-dev python3-dev libsystemd-dev build-essential pkg-config curl unzip
COPY ./ ./

RUN --mount=type=cache,target=/root/.cache/pip python -m pip install pygments==2.16.1
RUN pygmentize -S default -f html -a .highlight > csvbase/web/static/codehilite.css
RUN pygmentize -S lightbulb -f html -a .highlight > csvbase/web/static/codehilite-dark.css
RUN --mount=type=cache,target=/root/.cache/pip make static-deps

RUN --mount=type=cache,target=/root/.cache/pip python -m pip wheel -w wheelhouse .

FROM python:3.9-slim-bullseye
WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV TZ=UTC
ENV DEBIAN_FRONTEND=noninteractive
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone

ENV TINI_VERSION="v0.19.0"
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    apt-get update && apt-get -y install libpq5
COPY --from=builder /app/wheelhouse wheelhouse

RUN --mount=type=cache,target=/root/.cache/pip python -m pip --no-cache-dir install csvbase --no-index -f wheelhouse

ENV FLASK_APP=csvbase.web.app:init_app()
ENV FLASK_DEBUG=0
COPY alembic.ini .
COPY migrations migrations
EXPOSE 6001
ENTRYPOINT ["/tini", "--"]
CMD ["gunicorn", "csvbase.web.app:init_app()", "-b", ":6001"]

